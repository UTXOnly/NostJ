package nostj.websockethandler.config;

import nostj.websockethandler.handler.WebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.*;
import redis.clients.jedis.JedisPool;

import javax.sql.DataSource;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final DataSource dataSource;

    public WebSocketConfig(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Bean
    public JedisPool jedisPool() {
        String redisHost = System.getenv("REDIS_HOST");
        String redisPort = System.getenv("REDIS_PORT");

        if (redisHost == null || redisPort == null) {
            throw new IllegalStateException("REDIS_HOST or REDIS_PORT environment variable is not set.");
        }

        return new JedisPool(redisHost, Integer.parseInt(redisPort));
    }

    @Bean
    public WebSocketHandler webSocketHandler(JedisPool jedisPool) {
        return new WebSocketHandler(dataSource, jedisPool);
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(webSocketHandler(jedisPool()), "/").setAllowedOrigins("*");
    }
}
