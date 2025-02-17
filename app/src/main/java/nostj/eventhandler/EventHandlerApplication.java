package nostj.eventhandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

@SpringBootApplication
@RestController
public class EventHandlerApplication {

    private static final Logger logger = Logger.getLogger(EventHandlerApplication.class.getName());
    private final DataSource dataSource;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JedisPool jedisPool;
    private static final String REDIS_CHANNEL = "new_events_channel";
    private static final boolean WOT_ENABLED = Boolean.parseBoolean(System.getenv("WOT_ENABLED"));

    // ExecutorService for async tasks
    private static final ExecutorService executorService = Executors.newFixedThreadPool(10);

    public EventHandlerApplication(DataSource dataSource, JedisPool jedisPool) {
        this.dataSource = dataSource;
        this.jedisPool = jedisPool;
    }

    @Bean
    public static JedisPool jedisPool() {
        String redisHost = System.getenv("REDIS_HOST");
        String redisPort = System.getenv("REDIS_PORT");

        if (redisHost == null || redisPort == null) {
            throw new IllegalStateException("REDIS_HOST or REDIS_PORT environment variable is not set.");
        }

        return new JedisPool(redisHost, Integer.parseInt(redisPort));
    }

    @PostMapping(value = "/new_event", consumes = "*/*")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> handleNewEvent(
            @RequestBody(required = false) byte[] rawPayload,
            @RequestHeader(value = "Content-Type", required = false) String contentType) {

        return CompletableFuture.supplyAsync(() -> {
            logger.info("Received /new_event request with Content-Type: " + contentType);

            Map<String, Object> requestData = parseRequestBody(rawPayload, contentType);
            if (requestData == null) {
                logger.severe("Failed to parse request body - Invalid payload format");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                        "event", "ERROR",
                        "subscription_id", "unknown",
                        "results_json", "false",
                        "message", "error: empty request to event handler"
                ));
            }

            logger.info("New event request data: " + requestData);

            Map<String, Object> eventData = (Map<String, Object>) requestData.get("event_dict");
            if (eventData == null) {
                logger.severe("Missing 'event_dict' in request");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                        "event", "ERROR",
                        "subscription_id", "unknown",
                        "results_json", "false",
                        "message", "error: malformed event data"
                ));
            }

            Event event = new Event(eventData);
            logger.info("Verifying signature for event: " + event.getId());

            if (!event.verifySignature()) {
                logger.warning("Invalid signature for event: " + event.getId());
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                        "event", "ERROR",
                        "subscription_id", event.getId(),
                        "results_json", "false",
                        "message", "error: invalid event signature"
                ));
            }

            return processEvent(event, eventData);
        }, executorService);
    }

    private ResponseEntity<Map<String, Object>> processEvent(Event event, Map<String, Object> eventData) {
        try (Connection conn = dataSource.getConnection()) {
            logger.info("Connected to database. Processing event...");

            if (WOT_ENABLED && !event.checkWot(conn)) {
                logger.warning("Event rejected due to Web of Trust filter");
                return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of(
                        "event", "OK",
                        "subscription_id", event.getId(),
                        "results_json", "false",
                        "message", "error: forbidden - user not in web of trust"
                ));
            }

            if (event.getKind() == 5) {
                logger.info("Deleting event with kind=5");
                event.deleteEvent(conn);
                return ResponseEntity.ok(Map.of("event", "OK", "message", "Events deleted"));
            } else {
                if (event.addEvent(conn)) {
                    logger.info("Event successfully added to DB: " + event.getId());
                    publishToRedisAsync(eventData);
                    return ResponseEntity.ok(Map.of(
                            "event", "OK",
                            "subscription_id", event.getId(),
                            "results_json", "true",
                            "message", ""
                    ));
                } else {
                    logger.warning("Duplicate event detected: " + event.getId());
                    return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                            "event", "ERROR",
                            "message", "Duplicate event"
                    ));
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Database error: " + e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "event", "ERROR",
                    "message", "Database error"
            ));
        }
    }

    @PostMapping(value = "/subscription", consumes = "*/*")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> handleSubscription(
            @RequestBody(required = false) byte[] rawPayload,
            @RequestHeader(value = "Content-Type", required = false) String contentType) {

        return CompletableFuture.supplyAsync(() -> {
            logger.info("Received /subscription request with Content-Type: " + contentType);

            Map<String, Object> subscriptionData = parseRequestBody(rawPayload, contentType);
            if (subscriptionData == null) {
                logger.severe("Failed to parse request body - Invalid payload format");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "Invalid payload format"));
            }

            String subscriptionId = (String) subscriptionData.get("subscription_id");
            if (subscriptionId == null || subscriptionId.isEmpty()) {
                logger.severe("Missing subscription_id in request");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "subscription_id is required"));
            }

            return fetchSubscriptionEvents(subscriptionId);
        }, executorService);
    }

    private ResponseEntity<Map<String, Object>> fetchSubscriptionEvents(String subscriptionId) {
        try (Connection conn = dataSource.getConnection()) {
            Subscription subscription = new Subscription(Map.of("subscription_id", subscriptionId), jedisPool);
            List<Map<String, Object>> events = subscription.fetchEvents(conn);

            logger.info("Fetched " + events.size() + " events for subscription ID: " + subscriptionId);

            return ResponseEntity.ok(Map.of(
                    "event", "EVENT",
                    "subscription_id", subscriptionId,
                    "results_json", events
            ));
        } catch (SQLException e) {
            logger.severe("Database error: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "database failure",
                    "subscription_id", subscriptionId
            ));
        }
    }

    private void publishToRedisAsync(Map<String, Object> eventData) {
        CompletableFuture.runAsync(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                String jsonMessage = objectMapper.writeValueAsString(eventData);
                jedis.publish(REDIS_CHANNEL, jsonMessage);
                logger.info("Published event to Redis: " + jsonMessage);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error publishing to Redis: " + e.getMessage(), e);
            }
        }, executorService);
    }

    private Map<String, Object> parseRequestBody(byte[] rawPayload, String contentType) {
        try {
            if (rawPayload == null || rawPayload.length == 0) return null;
            return objectMapper.readValue(rawPayload, Map.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to parse request body: " + e.getMessage(), e);
            return null;
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(EventHandlerApplication.class, args);
    }
}
