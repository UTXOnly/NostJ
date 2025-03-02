package nostj.eventhandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
            return "query_cache:default";
        }
    }

    public CompletableFuture<List<Map<String, Object>>> fetchEvents(Pool pgPool) {
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
                return fetchFromDatabase(pgPool, cacheKey);
            }).exceptionally(ex -> {
                logger.warning("Redis fetch failed: " + ex.getMessage());
                return new ArrayList<>();
            });
    }

    private CompletableFuture<List<Map<String, Object>>> fetchFromDatabase(Pool pgPool, String cacheKey) {
        StringBuilder query = new StringBuilder("SELECT * FROM events");
        List<String> conditions = new ArrayList<>();
        Tuple params = Tuple.tuple();
        int paramIndex = 1;

        if (filters.containsKey("ids") && !((List<String>) filters.get("ids")).isEmpty()) {
            conditions.add("id = ANY($" + paramIndex + ")");
            params.addValue(((List<String>) filters.get("ids")).toArray(new String[0]));
            paramIndex++;
        }

        if (filters.containsKey("authors") && !((List<String>) filters.get("authors")).isEmpty()) {
            conditions.add("pubkey = ANY($" + paramIndex + ")");
            params.addValue(((List<String>) filters.get("authors")).toArray(new String[0]));
            paramIndex++;
        }

        if (filters.containsKey("kinds") && !((List<Integer>) filters.get("kinds")).isEmpty()) {
            conditions.add("kind = ANY($" + paramIndex + ")");
            params.addValue(((List<Integer>) filters.get("kinds")).toArray(new Integer[0]));
            paramIndex++;
        }

        if (filters.containsKey("since")) {
            conditions.add("created_at >= $" + paramIndex);
            params.addValue(((Number) filters.get("since")).longValue());
            paramIndex++;
        }

        if (filters.containsKey("until")) {
            conditions.add("created_at <= $" + paramIndex);
            params.addValue(((Number) filters.get("until")).longValue());
            paramIndex++;
        }

        if (!conditions.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", conditions));
        }

        query.append(" ORDER BY created_at DESC LIMIT 100");

        logger.info("Executing Query: " + query);
        logger.info("Query Parameters: " + params.deepToString());

        CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();

        pgPool.preparedQuery(query.toString())
            .execute(params)
            .onSuccess(rows -> {
                List<Map<String, Object>> results = new ArrayList<>();
                for (Row row : rows) {
                    Map<String, Object> event = new HashMap<>();
                    event.put("id", row.getString("id"));
                    event.put("pubkey", row.getString("pubkey"));
                    event.put("kind", row.getInteger("kind"));
                    event.put("created_at", row.getLong("created_at"));
                    event.put("tags", row.getJson("tags"));
                    event.put("content", row.getString("content"));
                    event.put("sig", row.getString("sig"));
                    results.add(event);
                }
                logger.info("Fetched " + results.size() + " events for filters: " + filters);
                future.complete(results);
                storeInCache(cacheKey, results);
            })
            .onFailure(ex -> {
                logger.severe("Database query failed: " + ex.getMessage());
                future.completeExceptionally(ex);
            });

        return future;
    }

    private void storeInCache(String cacheKey, List<Map<String, Object>> events) {
        try {
            String jsonData = objectMapper.writeValueAsString(events);
            redisAsync.setex(cacheKey, 240, jsonData)
                .toCompletableFuture()
                .thenAccept(status -> {
                    if ("OK".equals(status)) {
                        logger.info("Stored query results in Redis for key: " + cacheKey);
                    } else {
                        logger.warning("Failed to store results in Redis.");
                    }
                })
                .exceptionally(ex -> {
                    logger.warning("Redis storage failed: " + ex.getMessage());
                    return null;
                });
        } catch (JsonProcessingException e) {
            logger.warning("Failed to serialize query results for Redis: " + e.getMessage());
        }
    }
}
