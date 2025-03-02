package nostj.eventhandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class Subscription {
    private static final Logger logger = Logger.getLogger(Subscription.class.getName());
    private final Map<String, Object> filters;
    private final RedisAsyncCommands<String, String> redisAsync;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Subscription(Map<String, Object> subscriptionData, RedisAsyncCommands<String, String> redisAsync) {
        this.filters = subscriptionData;
        this.redisAsync = redisAsync;
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
        } catch (Exception e) {
            logger.severe("Error generating cache key: " + e.getMessage());
            return "query_cache:default"; // Fallback cache key
        }
    }

    /**
     * Fetches events from either Redis cache (if available) or the database.
     */
    public CompletableFuture<List<Map<String, Object>>> fetchEvents(Connection conn) {
        String cacheKey = generateCacheKey();

        return redisAsync.get(cacheKey).toCompletableFuture()
            .thenCompose(cachedResults -> {
                if (cachedResults != null) {
                    try {
                        List<Map<String, Object>> cachedEvents = objectMapper.readValue(cachedResults, List.class);
                        return CompletableFuture.completedFuture(cachedEvents);
                    } catch (JsonProcessingException e) {
                        logger.warning("Failed to parse cached results: " + e.getMessage());
                    }
                }
                return fetchFromDatabase(conn, cacheKey);
            }).exceptionally(ex -> {
                logger.warning("Redis fetch failed: " + ex.getMessage());
                return new ArrayList<>();
            });
    }

    private CompletableFuture<List<Map<String, Object>>> fetchFromDatabase(Connection conn, String cacheKey) {
        List<Map<String, Object>> events = new ArrayList<>();
        
        if (conn == null) {
            logger.severe("Database connection is null. Cannot execute query.");
            return CompletableFuture.completedFuture(events);
        }
    
        try {
            if (conn.isClosed()) {
                logger.severe("Database connection is closed. Attempting to reconnect...");
                return CompletableFuture.completedFuture(events);
            }
        } catch (SQLException e) {
            logger.severe("Error checking database connection state: " + e.getMessage());
            return CompletableFuture.completedFuture(events);
        }
    
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
    
        logger.info("Executing Query: " + query);
        logger.info("Query Parameters: " + params);
    
        return CompletableFuture.supplyAsync(() -> {
            try (Connection newConn = conn.isValid(2) ? conn : conn.getMetaData().getConnection();
                 PreparedStatement stmt = newConn.prepareStatement(query.toString())) {
    
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
                logger.info("Fetched " + events.size() + " events for filters: " + filters);
            } catch (SQLException e) {
                logger.severe("Database query failed: " + e.getMessage());
            }
            return events;
        }).thenCompose(eventList -> storeInCache(cacheKey, eventList));
    }
    

    private CompletableFuture<List<Map<String, Object>>> storeInCache(String cacheKey, List<Map<String, Object>> events) {
        try {
            String jsonData = objectMapper.writeValueAsString(events);
            return redisAsync.setex(cacheKey, 240, jsonData) // Cache for 4 minutes
                    .toCompletableFuture()
                    .thenApply(status -> {
                        if ("OK".equals(status)) {
                            logger.info("Stored query results in Redis for key: " + cacheKey);
                        } else {
                            logger.warning("Failed to store results in Redis.");
                        }
                        return events;
                    }).exceptionally(ex -> {
                        logger.warning("Redis storage failed: " + ex.getMessage());
                        return events;
                    });
        } catch (JsonProcessingException e) {
            logger.warning("Failed to serialize query results for Redis: " + e.getMessage());
            return CompletableFuture.completedFuture(events);
        }
    }
}
