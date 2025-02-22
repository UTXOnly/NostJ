package nostj.websockethandler.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import nostj.websockethandler.models.ExtractedResponseList;
import nostj.websockethandler.models.WebsocketMessages;
import nostj.eventhandler.Event;
import nostj.eventhandler.Subscription;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

@Component
public class WebSocketHandler extends TextWebSocketHandler {

    private static final Logger logger = Logger.getLogger(WebSocketHandler.class.getName());
    private static final String REDIS_CHANNEL = "new_events_channel";
    private final Map<String, WebSocketSession> activeSessions = new ConcurrentHashMap<>();
    private final Map<String, String> sessionSubscriptions = new ConcurrentHashMap<>();
    private final Map<String, WebSocketSession> clientSessions = new ConcurrentHashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final JedisPool jedisPool;
    private final DataSource dataSource;
    private static final boolean WOT_ENABLED = Boolean.parseBoolean(System.getenv("WOT_ENABLED"));
    private static final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private boolean redisListenerStarted = false;

    public WebSocketHandler(DataSource dataSource, JedisPool jedisPool) {
        this.dataSource = dataSource;
        this.jedisPool = jedisPool;
        startRedisListener();
    }

    
    private class RedisSubscriber extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
            if (!REDIS_CHANNEL.equals(channel)) return;

            CompletableFuture.runAsync(() -> {
                try {
                    Map<String, Object> eventData = objectMapper.readValue(message, Map.class);
                    for (Map.Entry<String, String> entry : sessionSubscriptions.entrySet()) {
                        WebSocketSession session = activeSessions.get(entry.getValue());
                        if (session != null && session.isOpen()) {
                            sendToClient(session, objectMapper.writeValueAsString(List.of("EVENT", entry.getKey(), eventData)));
                        }
                    }
                } catch (Exception e) {
                    logger.severe("Error processing Redis message: " + e.getMessage());
                }
            });
        }
    }

    public void startRedisListener() {
        if (redisListenerStarted) return;
        redisListenerStarted = true;

        CompletableFuture.runAsync(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(new RedisSubscriber(), REDIS_CHANNEL);
            } catch (Exception e) {
                logger.severe("Redis subscription error: " + e.getMessage());
            }
        });
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String clientId = getClientIdentifier(session);
        logger.info("New WebSocket connection: " + clientId);

        activeSessions.put(session.getId(), session);
        clientSessions.put(clientId, session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        CompletableFuture.runAsync(() -> {
            try {
                String clientId = getClientIdentifier(session);
                logger.info("WebSocket session closed: " + session.getId() + " | Client: " + clientId + " | Reason: " + status.getReason());

                activeSessions.remove(session.getId());
                clientSessions.remove(clientId);
                sessionSubscriptions.values().remove(session.getId());
            } catch (Exception e) {
                logger.severe("Error handling WebSocket closure: " + e.getMessage());
            }
        });
    }

    private void handleClose(WebSocketSession session) {
        CompletableFuture.runAsync(() -> {
            try {
                logger.info("Removing websocket subscriptions: " + session.getId());
                sessionSubscriptions.values().remove(session.getId());
            } catch (Exception e) {
                logger.severe("Error closing WebSocket session: " + e.getMessage());
            }
        });
    }

    private String getClientIdentifier(WebSocketSession session) {
        try {
            return session.getRemoteAddress().toString();
        } catch (Exception e) {
            return session.getId();
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        CompletableFuture.runAsync(() -> {
            try {
                logger.info("Received WebSocket message: " + message.getPayload());
                WebsocketMessages wsMessage = new WebsocketMessages(objectMapper.readValue(message.getPayload(), List.class));

                switch (wsMessage.getEventType()) {
                    case "REQ":
                        handleSubscription(wsMessage, session);
                        break;
                    case "EVENT":
                        handleEvent(wsMessage, session);
                        break;
                    case "CLOSE":
                        handleClose(session);
                        break;
                    default:
                        logger.warning("Unknown event type: " + wsMessage.getEventType());
                }
            } catch (Exception e) {
                logger.severe("Error processing WebSocket message: " + e.getMessage());
            }
        }, executorService);
    }

    private void handleSubscription(WebsocketMessages wsMessage, WebSocketSession session) {
        CompletableFuture.runAsync(() -> {
            try (Connection conn = dataSource.getConnection()) {
                logger.info("Processing subscription: " + wsMessage.getSubscriptionId());

                sessionSubscriptions.put(wsMessage.getSubscriptionId(), session.getId());

                Subscription subscription = new Subscription(wsMessage.getEventPayload(), jedisPool);
                List<Map<String, Object>> events = subscription.fetchEvents(conn);

                sendEventsToClient(events, wsMessage.getSubscriptionId(), session);
            } catch (SQLException e) {
                logger.severe("Error processing subscription: " + e.getMessage());
            }
        }, executorService);
    }

    private void handleEvent(WebsocketMessages wsMessage, WebSocketSession session) {
        CompletableFuture.runAsync(() -> {
            try (Connection conn = dataSource.getConnection()) {
                logger.info("Processing event: " + wsMessage.getEventPayload());

                Event event = new Event(wsMessage.getEventPayload());

                if (!event.verifySignature()) {
                    sendToClient(session, objectMapper.writeValueAsString(List.of("ERROR", "invalid signature")));
                    return;
                }

                if (WOT_ENABLED && !event.checkWot(conn)) {
                    sendToClient(session, objectMapper.writeValueAsString(List.of("ERROR", "User not in Web of Trust")));
                    return;
                }

                if (event.getKind() == 5) {
                    event.deleteEvent(conn);
                    sendToClient(session, objectMapper.writeValueAsString(List.of("OK", "Events deleted")));
                } else {
                    if (event.addEvent(conn)) {
                        publishToRedisAsync(event.getEventMap());
                        sendToClient(session, objectMapper.writeValueAsString(List.of("OK", event.getId())));
                    } else {
                        sendToClient(session, objectMapper.writeValueAsString(List.of("ERROR", "Duplicate event")));
                    }
                }
            } catch (Exception e) {
                logger.severe("Error processing event: " + e.getMessage());
            }
        }, executorService);
    }

    private void sendEventsToClient(List<Map<String, Object>> eventsList, String subscriptionId, WebSocketSession session) {
        CompletableFuture.runAsync(() -> {
            try {
                for (Map<String, Object> eventData : eventsList) {
                    sendToClient(session, objectMapper.writeValueAsString(List.of("EVENT", subscriptionId, eventData)));
                }
                sendToClient(session, objectMapper.writeValueAsString(List.of("EOSE", subscriptionId)));
            } catch (Exception e) {
                logger.severe("Error sending events to client: " + e.getMessage());
            }
        }, executorService);
    }

    private void sendToClient(WebSocketSession session, String message) {
        synchronized (session) {
            try {
                if (session.isOpen()) {
                    session.sendMessage(new TextMessage(message));
                } else {
                    logger.warning("Attempted to send message to closed session.");
                }
            } catch (Exception e) {
                logger.severe("Error sending WebSocket message: " + e.getMessage());
            }
        }
    }

    private void publishToRedisAsync(Map<String, Object> eventData) {
        CompletableFuture.runAsync(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.publish(REDIS_CHANNEL, objectMapper.writeValueAsString(eventData));
                logger.info("Published event to Redis.");
            } catch (Exception e) {
                logger.severe("Error publishing to Redis: " + e.getMessage());
            }
        }, executorService);
    }
}
