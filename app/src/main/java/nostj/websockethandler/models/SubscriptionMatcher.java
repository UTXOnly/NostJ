package nostj.websockethandler.models;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class SubscriptionMatcher {
    private final String subscriptionId;
    private final List<Map<String, Object>> filters;
    private final Logger logger;

    public SubscriptionMatcher(String subscriptionId, List<Map<String, Object>> filters, Logger logger) {
        this.subscriptionId = subscriptionId;
        this.filters = filters;
        this.logger = logger;
    }

    public boolean matchEvent(Map<String, Object> event) {
        for (Map<String, Object> filter : filters) {
            for (String key : filter.keySet()) {
                if (!event.containsKey(key) || !event.get(key).equals(filter.get(key))) {
                    return false;
                }
            }
        }
        return true;
    }
}
