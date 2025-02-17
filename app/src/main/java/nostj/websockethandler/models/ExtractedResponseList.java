package nostj.websockethandler.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;  // ✅ Fix: Add missing imports

public class ExtractedResponseList {
    @JsonProperty("event")
    private String event;

    @JsonProperty("subscription_id")
    private String subscriptionId;

    @JsonProperty("results_json")
    private List<Map<String, Object>> resultsJson;  // ✅ Keep your original variable name

    public String getEvent() {
        return event;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public List<Map<String, Object>> getResultsJson() {  // ✅ Keep your original method name
        return resultsJson;
    }
}
