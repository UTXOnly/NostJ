package nostj.websockethandler.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.websocket.Session;
import java.util.List;
import java.util.Map;

public class WebsocketMessages {

    private String eventType;
    private String subscriptionId;
    private Map<String, Object> eventPayload;

    public WebsocketMessages(List<Object> messageList) {
        if (messageList.size() < 2) {
            throw new IllegalArgumentException("Invalid WebSocket message format");
        }
        this.eventType = (String) messageList.get(0);

        if ("REQ".equals(eventType) || "CLOSE".equals(eventType)) {
            this.subscriptionId = (String) messageList.get(1);
            this.eventPayload = messageList.size() > 2 ? (Map<String, Object>) messageList.get(2) : null;
        } 
        else {
            this.eventPayload = (Map<String, Object>) messageList.get(1);
        }
    }

    public String getEventType() {
        return eventType;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public Map<String, Object> getEventPayload() {
        return eventPayload;
    }
}
