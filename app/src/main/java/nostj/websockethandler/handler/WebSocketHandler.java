package nostj.websockethandler.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import nostj.eventhandler.Subscription;
import nostj.eventhandler.Event;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class WebSocketHandler {

    private static final Logger logger = Logger.getLogger(WebSocketHandler.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String REDIS_CHANNEL = "new_events_channel";
    private static final boolean WOT_ENABLED = Boolean.parseBoolean(System.getenv("WOT_ENABLED"));

    private final int port;
    private final Pool pgPool;
    private final RedisClient redisClient;
    private final StatefulRedisPubSubConnection<String, String> redisConnection;
    private final RedisPubSubAsyncCommands<String, String> redisAsync;
    private final Map<Channel, Set<String>> sessionSubscriptions = new ConcurrentHashMap<>();

    public WebSocketHandler(Vertx vertx, int port, Pool pgPool, String redisUri) {
        this.port = port;
        this.pgPool = pgPool;
        this.redisClient = RedisClient.create(redisUri);
        this.redisConnection = redisClient.connectPubSub();
        this.redisAsync = redisConnection.async();
        startRedisListener();
    }

    private void startRedisListener() {
        redisConnection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                if (!REDIS_CHANNEL.equals(channel)) return;
                try {
                    Map<String, Object> eventData = objectMapper.readValue(message, Map.class);
                    sessionSubscriptions.forEach((channelCtx, subscriptions) -> {
                        for (String subId : subscriptions) {
                            sendToClient(channelCtx, List.of("EVENT", subId, eventData));
                        }
                    });
                } catch (Exception e) {
                    logger.severe("Error processing Redis message: " + e.getMessage());
                }
            }
        });
        redisAsync.subscribe(REDIS_CHANNEL);
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new HttpServerCodec(),
                                    new HttpObjectAggregator(65536),
                                    new ChunkedWriteHandler(),
                                    new WebSocketServerHandler()
                            );
                        }
                    });

            Channel channel = bootstrap.bind(port).sync().channel();
            logger.info("WebSocket server started on port " + port);
            channel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private void sendToClient(Channel channel, List<Object> message) {
        try {
            channel.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(message)));
        } catch (JsonProcessingException e) {
            logger.severe("Error serializing WebSocket message: " + e.getMessage());
        }
    }

    private class WebSocketServerHandler extends SimpleChannelInboundHandler<Object> {
        private WebSocketServerHandshaker handshaker;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof FullHttpRequest) {
                handleHttpRequest(ctx, (FullHttpRequest) msg);
            } else if (msg instanceof WebSocketFrame) {
                handleWebSocketFrame(ctx, (WebSocketFrame) msg);
            }
        }

        private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
            if (!req.decoderResult().isSuccess() || !"websocket".equals(req.headers().get("Upgrade"))) {
                ctx.close();
                return;
            }

            WebSocketServerHandshakerFactory factory = new WebSocketServerHandshakerFactory(
                    "ws://" + req.headers().get(HttpHeaderNames.HOST) + req.uri(), null, true);
            handshaker = factory.newHandshaker(req);
            if (handshaker == null) {
                WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
            } else {
                handshaker.handshake(ctx.channel(), req);
            }
        }

        private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame instanceof CloseWebSocketFrame) {
                handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
                return;
            }
            if (frame instanceof PingWebSocketFrame) {
                ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                return;
            }
            if (frame instanceof TextWebSocketFrame) {
                processMessage(ctx.channel(), ((TextWebSocketFrame) frame).text());
            }
        }

        private void processMessage(Channel channel, String message) {
            try {
                List<Object> parsedMessage = objectMapper.readValue(message, List.class);
                String eventType = (String) parsedMessage.get(0);
                switch (eventType) {
                    case "REQ":
                        handleSubscription(parsedMessage, channel);
                        break;
                    case "EVENT":
                        handleEvent(parsedMessage, channel);
                        break;
                    case "CLOSE":
                        handleUnsubscribe(parsedMessage, channel);
                        break;
                    default:
                        logger.warning("Unknown event type: " + eventType);
                }
            } catch (Exception e) {
                logger.severe("Error processing WebSocket message: " + e.getMessage());
            }
        }

        private void handleSubscription(List<Object> parsedMessage, Channel channel) {
            String subId = (String) parsedMessage.get(1);
            Map<String, Object> filter = (Map<String, Object>) parsedMessage.get(2);

            sessionSubscriptions.computeIfAbsent(channel, k -> new HashSet<>()).add(subId);
            Subscription subscription = new Subscription(filter, redisAsync);

            subscription.fetchEvents(pgPool)
                .thenAccept(events -> {
                    for (Map<String, Object> event : events) {
                        sendToClient(channel, List.of("EVENT", subId, event));
                    }
                    sendToClient(channel, List.of("EOSE", subId));
                })
                .exceptionally(ex -> {
                    logger.severe("Error processing subscription: " + ex.getMessage());
                    return null;
                });
        }

        private void handleEvent(List<Object> parsedMessage, Channel channel) {
            try {
                Map<String, Object> eventPayload = (Map<String, Object>) parsedMessage.get(1);
                Event event = new Event(eventPayload);

                if (!event.verifySignature()) {
                    sendToClient(channel, List.of("OK",event.getId(),"false", "invalid signature"));
                    return;
                }

                event.checkWot(pgPool).thenAccept(wotPassed -> {
                    if (WOT_ENABLED && !wotPassed) {
                        sendToClient(channel, List.of("OK",event.getId(),"false", "User not in Web of Trust"));
                        return;
                    }

                    if (event.getKind() == 5) {
                        event.deleteEvent(pgPool);
                        sendToClient(channel, List.of("OK", "Events deleted"));
                    } else {
                        event.addEvent(pgPool).thenAccept(inserted -> {
                            if (inserted) {
                                redisAsync.publish(REDIS_CHANNEL, event.getEventMap().toString());
                                sendToClient(channel, List.of("OK", event.getId(), "true", ""));
                            } else {
                                sendToClient(channel, List.of("OK",event.getId(),"false", "Duplicate event"));
                            }
                        });
                    }
                });

            } catch (Exception e) {
                logger.severe("Error handling WebSocket event: " + e.getMessage());
            }
        }

        private void handleUnsubscribe(List<Object> parsedMessage, Channel channel) {
            String subId = (String) parsedMessage.get(1);
            sessionSubscriptions.computeIfPresent(channel, (ch, subscriptions) -> {
                subscriptions.remove(subId);
                return subscriptions.isEmpty() ? null : subscriptions;
            });
            logger.info("Unsubscribed: " + subId);
        }
    }
}
