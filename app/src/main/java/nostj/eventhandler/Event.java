package nostj.eventhandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
        return true; // Implement actual verification logic here
    }

    public boolean checkWot(Connection conn) {
        String query = "SELECT pubkey FROM trust_network WHERE pubkey = ?";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, this.pubkey);
            try (ResultSet rs = stmt.executeQuery()) {
                boolean exists = rs.next();
                if (!exists) {
                    logger.warning("Event rejected due to Web of Trust filter for pubkey: " + this.pubkey);
                }
                return exists;
            }
        } catch (SQLException e) {
            logger.severe("Database error while checking WoT: " + e.getMessage());
            return false;
        }
    }

    public boolean addEvent(Connection conn) {
        String insertQuery = "INSERT INTO events (id, pubkey, kind, created_at, tags, content, sig) VALUES (?, ?, ?, ?, ?::jsonb, ?, ?) ON CONFLICT DO NOTHING";
        try (PreparedStatement stmt = conn.prepareStatement(insertQuery)) {
            stmt.setString(1, eventId);
            stmt.setString(2, pubkey);
            stmt.setInt(3, kind);
            stmt.setLong(4, createdAt);
            stmt.setString(5, convertTagsToJson());
            stmt.setString(6, content);
            stmt.setString(7, sig);

            int rowsInserted = stmt.executeUpdate();
            return rowsInserted > 0;
        } catch (SQLException e) {
            logger.severe("Error inserting event into DB: " + e.getMessage());
            return false;
        }
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

    public void deleteEvent(Connection conn) {
        String deleteQuery = "DELETE FROM events WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(deleteQuery)) {
            stmt.setString(1, this.eventId);
            int rowsDeleted = stmt.executeUpdate();
            logger.info("Deleted " + rowsDeleted + " event(s) with id: " + this.eventId);
        } catch (SQLException e) {
            logger.severe("Error deleting event from DB: " + e.getMessage());
        }
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
