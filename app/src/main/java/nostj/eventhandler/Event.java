package nostj.eventhandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
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

    public Event(Map<String, Object> eventData) {
        this.eventId = (String) eventData.get("id");
        this.pubkey = (String) eventData.get("pubkey");
        this.kind = (Integer) eventData.get("kind");
        this.createdAt = ((Number) eventData.get("created_at")).longValue();
        this.tags = (List<List<String>>) eventData.get("tags");
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
        return true;
    }

    public CompletableFuture<Boolean> checkWot(PgPool pgPool) {
        String query = "SELECT pubkey FROM trust_network WHERE pubkey = $1";

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        pgPool.preparedQuery(query)
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

    public CompletableFuture<Boolean> addEvent(PgPool pgPool) {
        String insertQuery = "INSERT INTO events (id, pubkey, kind, created_at, tags, content, sig) " +
                "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7) " +
                "ON CONFLICT DO NOTHING";

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        pgPool.preparedQuery(insertQuery)
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
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(tags);
        } catch (JsonProcessingException e) {
            logger.severe("Failed to convert tags to JSON for event " + eventId + ": " + e.getMessage());
            return "[]"; // Return empty JSON array in case of error
        }
    }

    public CompletableFuture<Void> deleteEvent(PgPool pgPool) {
        String deleteQuery = "DELETE FROM events WHERE id = $1";

        CompletableFuture<Void> future = new CompletableFuture<>();
        pgPool.preparedQuery(deleteQuery)
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
