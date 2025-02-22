package nostj.websockethandler.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
public class WebSocketTransportConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registry) {
        registry.setMessageSizeLimit(100 * 1024) // 64 KB message size limit
                .setSendBufferSizeLimit(300 * 1024) // 128 KB send buffer
                .setTimeToFirstMessage(5000); // 5 seconds timeout for first message
    }
}
