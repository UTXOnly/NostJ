package nostj.eventhandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.vertx.pgclient.PgPool;

import java.util.concurrent.ExecutionException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventHandlerApplication {

    private static final Logger logger = Logger.getLogger(EventHandlerApplication.class.getName());
    private final PgPool pgPool;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RedisAsyncCommands<String, String> redisAsync;
    private static final String REDIS_CHANNEL = "new_events_channel";
    private static final boolean WOT_ENABLED = Boolean.parseBoolean(System.getenv("WOT_ENABLED"));

    // ExecutorService for async tasks
    private static final ExecutorService executorService = Executors.newFixedThreadPool(10);

    public EventHandlerApplication(PgPool pgPool, RedisAsyncCommands<String, String> redisAsync) {
        this.pgPool = pgPool;
        this.redisAsync = redisAsync;
    }

    public CompletableFuture<Map<String, Object>> handleNewEvent(byte[] rawPayload, String contentType) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Received /new_event request with Content-Type: " + contentType);

            Map<String, Object> requestData = parseRequestBody(rawPayload);
            if (requestData == null) {
                return errorResponse("error: empty request to event handler");
            }

            logger.info("New event request data: " + requestData);

            Map<String, Object> eventData = (Map<String, Object>) requestData.get("event_dict");
            if (eventData == null) {
                return errorResponse("error: malformed event data");
            }

            return new Event(eventData);  // Ensure event is correctly inferred
        }, executorService).thenCompose(event -> processEvent((Event) event, ((Event) event).getEventMap()));
    }

    private CompletableFuture<Map<String, Object>> processEvent(Event event, Map<String, Object> eventData) {
        return CompletableFuture.supplyAsync(() -> {
            if (WOT_ENABLED) {
                logger.info("Checking Web of Trust for event: " + event.getId());
                return event.checkWot(pgPool);
            }
            return Boolean.TRUE; // Ensure boolean type
        }, executorService).thenCompose(wotPassed -> {
            if (Boolean.FALSE.equals(wotPassed)) {
                logger.warning("Event rejected due to Web of Trust filter");
                return CompletableFuture.completedFuture(errorResponse("error: forbidden - user not in web of trust"));
            }

            if (event.getKind() == 5) {
                logger.info("Deleting event with kind=5");
                return event.deleteEvent(pgPool).thenApply(deleted -> Map.of(
                        "event", "OK",
                        "message", "Events deleted"
                ));
            } else {
                return event.addEvent(pgPool).thenApply(inserted -> {
                    if (inserted) {
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
                        return errorResponse("Duplicate event");
                    }
                });
            }
        });
    }

    public CompletableFuture<Map<String, Object>> handleSubscription(byte[] rawPayload, String contentType) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Received /subscription request with Content-Type: " + contentType);

            Map<String, Object> subscriptionData = parseRequestBody(rawPayload);
            if (subscriptionData == null) {
                return errorResponse("error: Invalid payload format");
            }

            String subscriptionId = (String) subscriptionData.get("subscription_id");
            if (subscriptionId == null || subscriptionId.isEmpty()) {
                return errorResponse("error: subscription_id is required");
            }

            return subscriptionId;
        }, executorService).thenCompose(subscriptionId -> fetchSubscriptionEvents((String) subscriptionId));
    }

    private CompletableFuture<Map<String, Object>> fetchSubscriptionEvents(String subscriptionId) {
        Subscription subscription = new Subscription(Map.of("subscription_id", subscriptionId), redisAsync);
        return subscription.fetchEvents(pgPool).thenApply(events -> {
            logger.info("Fetched " + events.size() + " events for subscription ID: " + subscriptionId);

            return Map.of(
                    "event", "EVENT",
                    "subscription_id", subscriptionId,
                    "results_json", events
            );
        }).exceptionally(ex -> {
            logger.severe("Database error: " + ex.getMessage());
            return Map.of(
                    "error", "database failure",
                    "subscription_id", subscriptionId
            );
        });
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

    private Map<String, Object> errorResponse(String message) {
        return Map.of("event", "ERROR", "message", message);
    }
}
