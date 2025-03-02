package nostj.websockethandler;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import nostj.websockethandler.handler.WebSocketHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

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
        int port;
        try {
            port = Integer.parseInt(System.getenv("WS_PORT"));
        } catch (NumberFormatException e) {
            System.err.println("Invalid WS_PORT: " + System.getenv("WS_PORT") + ". Defaulting to 8008.");
            port = 8008;
        }

        // Construct Redis URI
        String redisHost = System.getenv("REDIS_HOST");
        String redisPort = System.getenv("REDIS_PORT");
        if (redisHost == null || redisPort == null) {
            System.err.println("ERROR: REDIS_HOST or REDIS_PORT is not set.");
            System.exit(1);
        }
        String redisUri = "redis://" + redisHost + ":" + redisPort;
        System.out.println("Using Redis URI: " + redisUri);

        // Initialize DataSource using HikariCP
        DataSource dataSource = createDataSource();

        // Validate PostgreSQL connection
        try (Connection conn = dataSource.getConnection()) {
            System.out.println("Successfully connected to PostgreSQL database.");
        } catch (SQLException e) {
            System.err.println("ERROR: Unable to connect to PostgreSQL database: " + e.getMessage());
            System.exit(1);
        }

        // Start WebSocket Server
        WebSocketHandler server = new WebSocketHandler(port, dataSource, redisUri);
        try {
            System.out.println("Starting WebSocket server on port " + port);
            server.start();
        } catch (InterruptedException e) {
            System.err.println("ERROR: WebSocket server interrupted.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static DataSource createDataSource() {
        String dbUrl = System.getenv("DB_URL");
        String dbUser = System.getenv("DB_USER");
        String dbPassword = System.getenv("DB_PASSWORD");

        if (dbUrl == null || dbUser == null || dbPassword == null) {
            System.err.println("ERROR: Database connection details (DB_URL, DB_USER, DB_PASSWORD) are not set.");
            System.exit(1);
        }

        System.out.println("Using database: " + dbUrl + " with user: " + dbUser);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUser);
        config.setPassword(dbPassword);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setIdleTimeout(30000);
        config.setMaxLifetime(600000);

        return new HikariDataSource(config);
    }
}
