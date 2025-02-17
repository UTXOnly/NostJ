package nostj.eventhandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class Subscription {
    private static final Logger logger = Logger.getLogger(Subscription.class.getName());
    private final Map<String, Object> filters;
    private final JedisPool jedisPool;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Subscription(Map<String, Object> subscriptionData, JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        this.filters = subscriptionData.containsKey("event_dict") ?
                (Map<String, Object>) ((List<?>) subscriptionData.get("event_dict")).get(0) :
                new HashMap<>();
    }

    /**
     * Generates a deterministic hash of the filter set to use as the Redis cache key.
     */
    private String generateCacheKey() {
        try {
            String jsonFilters = objectMapper.writeValueAsString(filters);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(jsonFilters.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                hexString.append(String.format("%02x", b));
            }
            return "query_cache:" + hexString;
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            logger.severe("Error generating cache key: " + e.getMessage());
            return "query_cache:default"; // Fallback cache key
        }
    }

    public List<Map<String, Object>> fetchEvents(Connection conn) throws SQLException {
        List<Map<String, Object>> events = new ArrayList<>();
        String cacheKey = generateCacheKey();

        // ✅ Check Redis cache first
        try (Jedis jedis = jedisPool.getResource()) {
            String cachedResults = jedis.get(cacheKey);
            if (cachedResults != null) {
                logger.info("Cache hit for filters: " + filters);
                return objectMapper.readValue(cachedResults, List.class);
            }
        } catch (Exception e) {
            logger.warning("Redis cache lookup failed: " + e.getMessage());
        }

        // ✅ Build SQL query dynamically
        StringBuilder query = new StringBuilder("SELECT * FROM events");
        List<Object> params = new ArrayList<>();
        List<String> conditions = new ArrayList<>();

        if (filters.containsKey("ids")) {
            List<String> ids = (List<String>) filters.get("ids");
            if (!ids.isEmpty()) {
                conditions.add("id = ANY(?)");
                params.add(ids.toArray(new String[0]));
            }
        }

        if (filters.containsKey("authors")) {
            List<String> authors = (List<String>) filters.get("authors");
            if (!authors.isEmpty()) {
                conditions.add("pubkey = ANY(?)");
                params.add(authors.toArray(new String[0]));
            }
        }

        if (filters.containsKey("kinds")) {
            List<Integer> kinds = (List<Integer>) filters.get("kinds");
            if (!kinds.isEmpty()) {
                conditions.add("kind = ANY(?)");
                params.add(kinds.toArray(new Integer[0]));
            }
        }

        if (filters.containsKey("since")) {
            conditions.add("created_at >= ?");
            params.add(((Number) filters.get("since")).longValue());
        }

        if (filters.containsKey("until")) {
            conditions.add("created_at <= ?");
            params.add(((Number) filters.get("until")).longValue());
        }

        if (!conditions.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", conditions));
        }

        query.append(" ORDER BY created_at DESC LIMIT 100");

        // ✅ Log the query before execution
        logger.info("Executing Query: " + query.toString());
        logger.info("Query Parameters: " + params);

        try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
            for (int i = 0; i < params.size(); i++) {
                if (params.get(i) instanceof Long) {
                    stmt.setLong(i + 1, (Long) params.get(i));
                } else if (params.get(i) instanceof Integer) {
                    stmt.setInt(i + 1, (Integer) params.get(i));
                } else {
                    stmt.setObject(i + 1, params.get(i));
                }
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> event = new HashMap<>();
                    event.put("id", rs.getString("id"));
                    event.put("pubkey", rs.getString("pubkey"));
                    event.put("kind", rs.getInt("kind"));
                    event.put("created_at", rs.getLong("created_at"));
                    event.put("tags", rs.getString("tags"));
                    event.put("content", rs.getString("content"));
                    event.put("sig", rs.getString("sig"));
                    events.add(event);
                }
            }
        }

        logger.info("Fetched " + events.size() + " events for filters: " + filters);

        // ✅ Store results in Redis cache
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(cacheKey, 240, objectMapper.writeValueAsString(events));  // Cache for 4 minutes
            logger.info("Stored query results in Redis for key: " + cacheKey);
        } catch (Exception e) {
            logger.warning("Failed to store results in Redis: " + e.getMessage());
        }

        return events;
    }
}
