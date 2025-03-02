package nostj.eventhandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Tuple;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class Event {
    private String eventId;
    private String pubkey;
    private int kind;
    private long createdAt;
    private List<List<String>> tags;
    private String content;
    private String sig;
    private static final Logger logger = Logger.getLogger(Event.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Event(Map<String, Object> eventData) {
        this.eventId = (String) eventData.get("id");
        this.pubkey = (String) eventData.get("pubkey");
        this.kind = (Integer) eventData.get("kind");
        this.createdAt = ((Number) eventData.get("created_at")).longValue();
        
        Object rawTags = eventData.get("tags");
        if (rawTags instanceof List) {
            this.tags = (List<List<String>>) rawTags;
        } else {
            this.tags = new ArrayList<>();
            logger.warning("Invalid or missing tags for event: " + eventId);
        }

        this.content = (String) eventData.get("content");
        this.sig = (String) eventData.get("sig");
    }

    public String getId() {
        return eventId;
    }

    public int getKind() {
        return kind;
    }

    public boolean verifySignature() {
        logger.info("Verifying signature for event: " + eventId);
        // TODO: Implement actual signature verification logic
        return true;
    }

    public CompletableFuture<Boolean> checkWot(Pool pool) {
        String query = "SELECT pubkey FROM trust_network WHERE pubkey = $1";
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        pool.preparedQuery(query)
            .execute(Tuple.of(this.pubkey))
            .onSuccess(rows -> {
                boolean exists = rows.iterator().hasNext();
                if (!exists) {
                    logger.warning("Event rejected due to Web of Trust filter for pubkey: " + this.pubkey);
                }
                future.complete(exists);
            })
            .onFailure(err -> {
                logger.severe("Database error while checking WoT: " + err.getMessage());
                future.complete(false);
            });

        return future;
    }

    public CompletableFuture<Boolean> addEvent(Pool pool) {
        String insertQuery = "INSERT INTO events (id, pubkey, kind, created_at, tags, content, sig) " +
                "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7) " +
                "ON CONFLICT DO NOTHING";

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        pool.preparedQuery(insertQuery)
            .execute(Tuple.of(eventId, pubkey, kind, createdAt, convertTagsToJson(), content, sig))
            .onSuccess(rows -> {
                boolean inserted = rows.rowCount() > 0;
                if (inserted) {
                    logger.info("Event successfully added to DB: " + eventId);
                } else {
                    logger.warning("Duplicate event detected: " + eventId);
                }
                future.complete(inserted);
            })
            .onFailure(err -> {
                logger.severe("Error inserting event into DB: " + err.getMessage());
                future.complete(false);
            });

        return future;
    }

    private String convertTagsToJson() {
        try {
            return objectMapper.writeValueAsString(tags);
        } catch (JsonProcessingException e) {
            logger.severe("Failed to convert tags to JSON for event " + eventId + ": " + e.getMessage());
            return "[]";
        }
    }

    public CompletableFuture<Void> deleteEvent(Pool pool) {
        String deleteQuery = "DELETE FROM events WHERE id = $1";
        CompletableFuture<Void> future = new CompletableFuture<>();

        pool.preparedQuery(deleteQuery)
            .execute(Tuple.of(this.eventId))
            .onSuccess(rows -> {
                logger.info("Deleted " + rows.rowCount() + " event(s) with id: " + this.eventId);
                future.complete(null);
            })
            .onFailure(err -> {
                logger.severe("Error deleting event from DB: " + err.getMessage());
                future.completeExceptionally(err);
            });

        return future;
    }

    public Map<String, Object> getEventMap() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("id", eventId);
        eventMap.put("pubkey", pubkey);
        eventMap.put("kind", kind);
        eventMap.put("created_at", createdAt);
        eventMap.put("tags", tags);
        eventMap.put("content", content);
        eventMap.put("sig", sig);
        return eventMap;
    }
}
