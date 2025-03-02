package nostj.websockethandler.models;

import java.util.List;
import java.util.Map;

public class WebsocketMessages {

    private final String eventType;
    private final String subscriptionId;
    private final Map<String, Object> eventPayload;

    public WebsocketMessages(List<Object> messageList) {
        if (messageList == null || messageList.size() < 2) {
            throw new IllegalArgumentException("Invalid WebSocket message format");
        }

        this.eventType = String.valueOf(messageList.get(0));

        if ("REQ".equals(eventType) || "CLOSE".equals(eventType)) {
            this.subscriptionId = messageList.get(1) instanceof String ? (String) messageList.get(1) : null;
            this.eventPayload = (messageList.size() > 2 && messageList.get(2) instanceof Map)
                    ? (Map<String, Object>) messageList.get(2)
                    : null;
        } else {
            this.subscriptionId = null;
            this.eventPayload = messageList.get(1) instanceof Map ? (Map<String, Object>) messageList.get(1) : null;
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
