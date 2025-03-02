package nostj.eventhandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.api.async.RedisAsyncCommands;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventHandlerApplication {

    private static final Logger logger = Logger.getLogger(EventHandlerApplication.class.getName());
    private final DataSource dataSource;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RedisAsyncCommands<String, String> redisAsync;
    private static final String REDIS_CHANNEL = "new_events_channel";
    private static final boolean WOT_ENABLED = Boolean.parseBoolean(System.getenv("WOT_ENABLED"));

    // ExecutorService for async tasks
    private static final ExecutorService executorService = Executors.newFixedThreadPool(10);

    public EventHandlerApplication(DataSource dataSource, RedisAsyncCommands<String, String> redisAsync) {
        this.dataSource = dataSource;
        this.redisAsync = redisAsync;
    }

    public CompletableFuture<Map<String, Object>> handleNewEvent(byte[] rawPayload, String contentType) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Received /new_event request with Content-Type: " + contentType);

            Map<String, Object> requestData = parseRequestBody(rawPayload);
            if (requestData == null) {
                logger.severe("Failed to parse request body - Invalid payload format");
                return Map.of(
                        "event", "ERROR",
                        "subscription_id", "unknown",
                        "results_json", "false",
                        "message", "error: empty request to event handler"
                );
            }

            logger.info("New event request data: " + requestData);

            Map<String, Object> eventData = (Map<String, Object>) requestData.get("event_dict");
            if (eventData == null) {
                logger.severe("Missing 'event_dict' in request");
                return Map.of(
                        "event", "ERROR",
                        "subscription_id", "unknown",
                        "results_json", "false",
                        "message", "error: malformed event data"
                );
            }

            Event event = new Event(eventData);
            logger.info("Verifying signature for event: " + event.getId());

            if (!event.verifySignature()) {
                logger.warning("Invalid signature for event: " + event.getId());
                return Map.of(
                        "event", "ERROR",
                        "subscription_id", event.getId(),
                        "results_json", "false",
                        "message", "error: invalid event signature"
                );
            }

            return processEvent(event, eventData);
        }, executorService);
    }

    private Map<String, Object> processEvent(Event event, Map<String, Object> eventData) {
        try (Connection conn = dataSource.getConnection()) {
            logger.info("Connected to database. Processing event...");

            if (WOT_ENABLED && !event.checkWot(conn)) {
                logger.warning("Event rejected due to Web of Trust filter");
                return Map.of(
                        "event", "OK",
                        "subscription_id", event.getId(),
                        "results_json", "false",
                        "message", "error: forbidden - user not in web of trust"
                );
            }

            if (event.getKind() == 5) {
                logger.info("Deleting event with kind=5");
                event.deleteEvent(conn);
                return Map.of("event", "OK", "message", "Events deleted");
            } else {
                if (event.addEvent(conn)) {
                    logger.info("Event successfully added to DB: " + event.getId());
                    publishToRedisAsync(eventData);
                    return Map.of(
                            "event", "OK",
                            "subscription_id", event.getId(),
                            "results_json", "true",
                            "message", ""
                    );
                } else {
                    logger.warning("Duplicate event detected: " + event.getId());
                    return Map.of(
                            "event", "ERROR",
                            "message", "Duplicate event"
                    );
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Database error: " + e.getMessage(), e);
            return Map.of(
                    "event", "ERROR",
                    "message", "Database error"
            );
        }
    }

    public CompletableFuture<Map<String, Object>> handleSubscription(byte[] rawPayload, String contentType) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Received /subscription request with Content-Type: " + contentType);

            Map<String, Object> subscriptionData = parseRequestBody(rawPayload);
            if (subscriptionData == null) {
                logger.severe("Failed to parse request body - Invalid payload format");
                return Map.of("error", "Invalid payload format");
            }

            String subscriptionId = (String) subscriptionData.get("subscription_id");
            if (subscriptionId == null || subscriptionId.isEmpty()) {
                logger.severe("Missing subscription_id in request");
                return Map.of("error", "subscription_id is required");
            }

            return fetchSubscriptionEvents(subscriptionId);
        }, executorService);
    }

    private Map<String, Object> fetchSubscriptionEvents(String subscriptionId) {
        try (Connection conn = dataSource.getConnection()) {
            Subscription subscription = new Subscription(Map.of("subscription_id", subscriptionId), redisAsync);
            List<Map<String, Object>> events = subscription.fetchEvents(conn).get();

            logger.info("Fetched " + events.size() + " events for subscription ID: " + subscriptionId);

            return Map.of(
                    "event", "EVENT",
                    "subscription_id", subscriptionId,
                    "results_json", events
            );
        } catch (SQLException | InterruptedException | ExecutionException e) {
            logger.severe("Database error: " + e.getMessage());
            return Map.of(
                    "error", "database failure",
                    "subscription_id", subscriptionId
            );
        }
    }

    private void publishToRedisAsync(Map<String, Object> eventData) {
        redisAsync.publish(REDIS_CHANNEL, serializeJson(eventData)).thenAccept(result -> {
            if (result > 0) {
                logger.info("Published event to Redis: " + eventData);
            } else {
                logger.warning("No Redis subscribers for event: " + eventData);
            }
        });
    }

    private Map<String, Object> parseRequestBody(byte[] rawPayload) {
        try {
            if (rawPayload == null || rawPayload.length == 0) return null;
            return objectMapper.readValue(rawPayload, Map.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to parse request body: " + e.getMessage(), e);
            return null;
        }
    }

    private String serializeJson(Map<String, Object> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to serialize JSON", e);
            return "{}";
        }
    }
}
