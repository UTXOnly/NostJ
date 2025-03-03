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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.common.AttributeKey;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class WebSocketHandler {

    private static final Logger logger = Logger.getLogger(WebSocketHandler.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String REDIS_CHANNEL = "new_events_channel";
    private static final boolean WOT_ENABLED = Boolean.parseBoolean(System.getenv("WOT_ENABLED"));

    private static final OpenTelemetry openTelemetry = initOpenTelemetry();
    private static final Tracer tracer = openTelemetry.getTracer("WebSocketHandler");

    private static OpenTelemetry initOpenTelemetry() {
        return OpenTelemetrySdk.builder()
            .setTracerProvider(SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(
                            OtlpGrpcSpanExporter.builder()
                                    .setEndpoint("http://datadog-agent:4317")
                                    .build()))
                    .build())
            .build();
    }

    private final int port;
    private final Pool pgPool;
    private final RedisClient redisClient;
    private final StatefulRedisPubSubConnection<String, String> redisConnection;
    private final RedisPubSubAsyncCommands<String, String> redisAsync;
    private final Map<Channel, Set<String>> sessionSubscriptions = new ConcurrentHashMap<>();
    private final Map<Channel, Context> channelContexts = new ConcurrentHashMap<>();

    public WebSocketHandler(Vertx vertx, int port, Pool pgPool, String redisUri) {
        this.port = port;
        this.pgPool = pgPool;
        this.redisClient = RedisClient.create(redisUri);
        this.redisConnection = redisClient.connectPubSub();
        this.redisAsync = redisConnection.async();
        startRedisListener();
    }

    private void startRedisListener() {
        // Create a single span for Redis listener initialization
        Span span = tracer.spanBuilder("redis.initialize_listener").startSpan();
        try (Scope scope = span.makeCurrent()) {
            redisConnection.addListener(new RedisPubSubAdapter<>() {
                @Override
                public void message(String channel, String message) {
                    if (!REDIS_CHANNEL.equals(channel)) return;
                    
                    // Create a span for handling Redis messages
                    Span redisMessageSpan = tracer.spanBuilder("redis.message")
                            .startSpan();
                    try (Scope redisScope = redisMessageSpan.makeCurrent()) {
                        redisMessageSpan.setAttribute("redis.channel", channel);
                        
                        try {
                            Map<String, Object> eventData = objectMapper.readValue(message, Map.class);
                            redisMessageSpan.setAttribute("event.id", eventData.getOrDefault("id", "unknown").toString());
                            
                            sessionSubscriptions.forEach((channelCtx, subscriptions) -> {
                                // Don't create spans for each channel, just send the events
                                for (String subId : subscriptions) {
                                    sendToClient(channelCtx, List.of("EVENT", subId, eventData));
                                }
                            });
                        } catch (Exception e) {
                            redisMessageSpan.recordException(e);
                            redisMessageSpan.setStatus(StatusCode.ERROR, e.getMessage());
                            logger.severe("Error processing Redis message: " + e.getMessage());
                        }
                    } finally {
                        redisMessageSpan.end();
                    }
                }
            });
            redisAsync.subscribe(REDIS_CHANNEL);
        } finally {
            span.end();
        }
    }

    public void start() throws InterruptedException {
        // Single span for server startup
        Span serverStartSpan = tracer.spanBuilder("server.start").startSpan();
        try (Scope scope = serverStartSpan.makeCurrent()) {
            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap bootstrap = new ServerBootstrap()
                        .group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                // No span needed for each channel initialization
                                ch.pipeline().addLast(
                                        new HttpServerCodec(),
                                        new HttpObjectAggregator(65536),
                                        new ChunkedWriteHandler(),
                                        new WebSocketServerHandler()
                                );
                            }
                        });

                Channel channel = bootstrap.bind(port).sync().channel();
                serverStartSpan.setAttribute("server.port", port);
                logger.info("WebSocket server started on port " + port);
                channel.closeFuture().sync();
            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        } finally {
            serverStartSpan.end();
        }
    }

    private void sendToClient(Channel channel, List<Object> message) {
        // No span for sendToClient to reduce noise
        try {
            String serializedMessage = objectMapper.writeValueAsString(message);
            channel.writeAndFlush(new TextWebSocketFrame(serializedMessage));
        } catch (JsonProcessingException e) {
            logger.severe("Error serializing WebSocket message: " + e.getMessage());
        }
    }

    private class WebSocketServerHandler extends SimpleChannelInboundHandler<Object> {
        private WebSocketServerHandshaker handshaker;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            // One span per client connection
            Span connectionSpan = tracer.spanBuilder("connection.client_connect")
                    .setAttribute("client.remoteAddress", ctx.channel().remoteAddress().toString())
                    .startSpan();
            
            try (Scope scope = connectionSpan.makeCurrent()) {
                // Store connection context
                channelContexts.put(ctx.channel(), Context.current());
                super.channelActive(ctx);
            } finally {
                connectionSpan.end(); // End the span right away, no need to track connection lifetime
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            // One span per client disconnection
            Span disconnectSpan = tracer.spanBuilder("connection.client_disconnect").startSpan();
            try (Scope scope = disconnectSpan.makeCurrent()) {
                sessionSubscriptions.remove(ctx.channel());
                channelContexts.remove(ctx.channel());
                super.channelInactive(ctx);
            } finally {
                disconnectSpan.end();
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            // No span here, we'll create spans in the specific handlers
            if (msg instanceof FullHttpRequest) {
                handleHttpRequest(ctx, (FullHttpRequest) msg);
            } else if (msg instanceof WebSocketFrame) {
                handleWebSocketFrame(ctx, (WebSocketFrame) msg);
            }
        }

        private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
            // Only create span for HTTP upgrade requests
            Span httpSpan = tracer.spanBuilder("http.upgrade_request").startSpan();
            try (Scope scope = httpSpan.makeCurrent()) {
                httpSpan.setAttribute("http.method", req.method().toString());
                httpSpan.setAttribute("http.url", req.uri());
                
                if (!req.decoderResult().isSuccess() || !"websocket".equals(req.headers().get("Upgrade"))) {
                    httpSpan.setStatus(StatusCode.ERROR, "Invalid HTTP request");
                    ctx.close();
                    return;
                }

                WebSocketServerHandshakerFactory factory = new WebSocketServerHandshakerFactory(
                        "ws://" + req.headers().get(HttpHeaderNames.HOST) + req.uri(), null, true);
                handshaker = factory.newHandshaker(req);
                if (handshaker == null) {
                    httpSpan.setStatus(StatusCode.ERROR, "Unsupported WebSocket version");
                    WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
                } else {
                    handshaker.handshake(ctx.channel(), req);
                    // Store context after successful handshake
                    channelContexts.put(ctx.channel(), Context.current());
                }
            } finally {
                httpSpan.end();
            }
        }

        private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
            // No span for frame handling, only create spans for actual messages
            if (frame instanceof CloseWebSocketFrame) {
                handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
                return;
            }
            if (frame instanceof PingWebSocketFrame) {
                ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                return;
            }
            if (frame instanceof TextWebSocketFrame) {
                String text = ((TextWebSocketFrame) frame).text();
                // Process the actual message
                processMessage(ctx.channel(), text);
            }
        }

        private void processMessage(Channel channel, String message) {
            // PRIMARY SPAN: One span per message received
            Span processSpan = tracer.spanBuilder("ws.message")
                    .setAttribute("message.size", message.length())
                    .startSpan();
            
            try (Scope scope = processSpan.makeCurrent()) {
                try {
                    List<Object> parsedMessage = objectMapper.readValue(message, List.class);
                    String eventType = (String) parsedMessage.get(0);
                    processSpan.setAttribute("message.type", eventType);
                    
                    // Store the context for this message processing
                    channelContexts.put(channel, Context.current());
                    
                    // Handle the message based on its type
                    switch (eventType) {
                        case "REQ":
                            handleSubscription(parsedMessage, channel, processSpan);
                            break;
                        case "EVENT":
                            handleEvent(parsedMessage, channel, processSpan);
                            break;
                        case "CLOSE":
                            handleUnsubscribe(parsedMessage, channel, processSpan);
                            break;
                        default:
                            processSpan.setStatus(StatusCode.ERROR, "Unknown event type");
                            logger.warning("Unknown event type: " + eventType);
                    }
                } catch (Exception e) {
                    processSpan.recordException(e);
                    processSpan.setStatus(StatusCode.ERROR, "Message processing error");
                    logger.severe("Error processing WebSocket message: " + e.getMessage());
                }
            } finally {
                processSpan.end();
            }
        }

        private void handleSubscription(List<Object> parsedMessage, Channel channel, Span parentSpan) {
            String subId = (String) parsedMessage.get(1);
            Map<String, Object> filter = (Map<String, Object>) parsedMessage.get(2);

            // Add attributes to parent span instead of creating a new one
            parentSpan.setAttribute("subscription.id", subId);
            parentSpan.setAttribute("subscription.filter", filter.toString());
            
            // Store the subscription in memory
            sessionSubscriptions.computeIfAbsent(channel, k -> new HashSet<>()).add(subId);
            
            // Create the subscription
            Subscription subscription = new Subscription(filter, redisAsync);
            
            // Save the current context for async operations
            Context messageContext = Context.current();

            // Fetch events - this is async so we'll create a new span for it
            subscription.fetchEvents(pgPool)
                .thenAccept(events -> {
                    // Create a span for the events fetch completion
                    Span fetchEventsSpan = tracer.spanBuilder("db.fetch_events")
                            .setParent(messageContext)
                            .setAttribute("events.count", events.size())
                            .setAttribute("subscription.id", subId)
                            .startSpan();
                    
                    try (Scope eventScope = fetchEventsSpan.makeCurrent()) {
                        // Send each event to the client
                        for (Map<String, Object> event : events) {
                            sendToClient(channel, List.of("EVENT", subId, event));
                        }
                        // Send end of stored events
                        sendToClient(channel, List.of("EOSE", subId));
                    } finally {
                        fetchEventsSpan.end();
                    }
                })
                .exceptionally(ex -> {
                    // Create a span for errors
                    Span errorSpan = tracer.spanBuilder("error.subscription")
                            .setParent(messageContext)
                            .setAttribute("subscription.id", subId)
                            .startSpan();
                    
                    try (Scope errorScope = errorSpan.makeCurrent()) {
                        errorSpan.recordException(ex);
                        errorSpan.setStatus(StatusCode.ERROR, "Error processing subscription");
                        logger.severe("Error processing subscription: " + ex.getMessage());
                    } finally {
                        errorSpan.end();
                    }
                    return null;
                });
        }
        
        private void handleEvent(List<Object> parsedMessage, Channel channel, Span parentSpan) {
            Map<String, Object> eventPayload = (Map<String, Object>) parsedMessage.get(1);
            Event event = new Event(eventPayload);

            // Add attributes to parent span
            parentSpan.setAttribute("event.id", event.getId());
            parentSpan.setAttribute("event.kind", event.getKind());
            
            // Store context for async operations
            Context eventContext = Context.current();

            // Verify signature synchronously
            if (!event.verifySignature()) {
                parentSpan.addEvent("Invalid signature");
                sendToClient(channel, List.of("OK", event.getId(), "false", "Invalid signature"));
                return;
            }

            // WOT check is async
            event.checkWot(pgPool).thenAccept(wotPassed -> {
                // Create a span for WOT check completion
                Span wotSpan = tracer.spanBuilder("wot.check")
                        .setParent(eventContext)
                        .setAttribute("event.id", event.getId())
                        .setAttribute("wot.passed", wotPassed)
                        .startSpan();
                
                try (Scope wotScope = wotSpan.makeCurrent()) {
                    if (WOT_ENABLED && !wotPassed) {
                        wotSpan.addEvent("WOT check failed");
                        sendToClient(channel, List.of("OK", event.getId(), "false", "User not in Web of Trust"));
                        return;
                    }
                    
                    // Store context for next async operation
                    Context wotContext = Context.current();

                    // Add event to database
                    event.addEvent(pgPool).thenAccept(inserted -> {
                        // Create a span for database operation completion
                        Span addEventSpan = tracer.spanBuilder("db.add_event")
                                .setParent(wotContext)
                                .setAttribute("event.id", event.getId())
                                .setAttribute("event.inserted", inserted)
                                .startSpan();
                        
                        try (Scope addEventScope = addEventSpan.makeCurrent()) {
                            if (inserted) {
                                // Publish to Redis and notify client
                                redisAsync.publish(REDIS_CHANNEL, event.getEventMap().toString());
                                sendToClient(channel, List.of("OK", event.getId(), "true", ""));
                            } else {
                                sendToClient(channel, List.of("OK", event.getId(), "false", "Duplicate event"));
                            }
                        } finally {
                            addEventSpan.end();
                        }
                    }).exceptionally(ex -> {
                        // Create a span for database errors
                        Span errorSpan = tracer.spanBuilder("error.db")
                                .setParent(wotContext)
                                .setAttribute("event.id", event.getId())
                                .startSpan();
                        
                        try (Scope errorScope = errorSpan.makeCurrent()) {
                            errorSpan.recordException(ex);
                            errorSpan.setStatus(StatusCode.ERROR, "Error adding event");
                            logger.severe("Error adding event: " + ex.getMessage());
                            sendToClient(channel, List.of("OK", event.getId(), "false", "Error adding event"));
                        } finally {
                            errorSpan.end();
                        }
                        return null;
                    });
                } finally {
                    wotSpan.end();
                }
            }).exceptionally(ex -> {
                // Create a span for WOT check errors
                Span errorSpan = tracer.spanBuilder("error.wot")
                        .setParent(eventContext)
                        .setAttribute("event.id", event.getId())
                        .startSpan();
                
                try (Scope errorScope = errorSpan.makeCurrent()) {
                    errorSpan.recordException(ex);
                    errorSpan.setStatus(StatusCode.ERROR, "Error checking WOT");
                    logger.severe("Error checking WOT: " + ex.getMessage());
                    sendToClient(channel, List.of("OK", event.getId(), "false", "Error checking WOT"));
                } finally {
                    errorSpan.end();
                }
                return null;
            });
        }

        private void handleUnsubscribe(List<Object> parsedMessage, Channel channel, Span parentSpan) {
            String subId = (String) parsedMessage.get(1);
            
            // Add attributes to parent span instead of creating a new one
            parentSpan.setAttribute("subscription.id", subId);
            
            // Remove the subscription
            sessionSubscriptions.computeIfPresent(channel, (ch, subscriptions) -> {
                subscriptions.remove(subId);
                return subscriptions.isEmpty() ? null : subscriptions;
            });
            
            logger.info("Unsubscribed: " + subId);
        }
    }
}