package nostj.websockethandler;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import nostj.websockethandler.handler.WebSocketHandler;

public class WebSocketApplication {
    public static void main(String[] args) {
        System.out.println("Starting WebSocketApplication...");
    
        // Print environment variables for debugging
        System.out.println("REDIS_HOST: " + System.getenv("REDIS_HOST"));
        System.out.println("REDIS_PORT: " + System.getenv("REDIS_PORT"));
        System.out.println("WS_PORT: " + System.getenv("WS_PORT"));
        System.out.println("DB_URL: " + System.getenv("DB_URL"));
        System.out.println("DB_USER: " + System.getenv("DB_USER"));
        System.out.println("DB_PASSWORD: " + (System.getenv("DB_PASSWORD") != null ? "******" : "NOT SET"));
    
        // Validate and parse port
        int port = parsePort(System.getenv("WS_PORT"), 8008);
    
        // Construct Redis URI
        String redisHost = System.getenv("REDIS_HOST");
        String redisPort = System.getenv("REDIS_PORT");
        if (redisHost == null || redisPort == null) {
            System.err.println("ERROR: REDIS_HOST or REDIS_PORT is not set.");
            System.exit(1);
        }
        String redisUri = "redis://" + redisHost + ":" + redisPort;
        System.out.println("Using Redis URI: " + redisUri);
    
        // Initialize Vert.x and PostgreSQL connection pool
        Vertx vertx = Vertx.vertx();
        Pool pgPool = createPgPool(vertx);
    
        // Start WebSocket Server
        WebSocketHandler server = new WebSocketHandler(vertx, port, pgPool, redisUri);
        System.out.println("Starting WebSocket server on port " + port);
        
        try {
            server.start();
        } catch (InterruptedException e) {
            System.err.println("WebSocket server was interrupted: " + e.getMessage());
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }
    }
    

    private static Pool createPgPool(Vertx vertx) {
        String dbUrl = System.getenv("DB_URL").replace("jdbc:postgresql://", ""); // Remove JDBC prefix
        String dbUser = System.getenv("DB_USER");
        String dbPassword = System.getenv("DB_PASSWORD");

        if (dbUrl == null || dbUser == null || dbPassword == null) {
            System.err.println("ERROR: Database connection details (DB_URL, DB_USER, DB_PASSWORD) are not set.");
            System.exit(1);
        }

        System.out.println("Using database: " + dbUrl + " with user: " + dbUser);

        PgConnectOptions connectOptions = new PgConnectOptions()
            .setHost(dbUrl.split(":")[0])  // Extract host
            .setPort(Integer.parseInt(dbUrl.split(":")[1].split("/")[0])) // Extract port
            .setDatabase(dbUrl.split("/")[1]) // Extract database name
            .setUser(dbUser)
            .setPassword(dbPassword)
            .setCachePreparedStatements(true);

        PoolOptions poolOptions = new PoolOptions().setMaxSize(10);
        return Pool.pool(vertx, connectOptions, poolOptions);
    }

    private static int parsePort(String portStr, int defaultPort) {
        try {
            return Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            System.err.println("Invalid WS_PORT: " + portStr + ". Defaulting to " + defaultPort);
            return defaultPort;
        }
    }
}
