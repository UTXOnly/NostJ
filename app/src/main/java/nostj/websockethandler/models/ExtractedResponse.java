package nostj.websockethandler.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExtractedResponse {
    @JsonProperty("event")
    private String event;

    @JsonProperty("subscription_id")
    private String subscriptionId;

    @JsonProperty("results_json")
    private String resultsJson;


    @JsonProperty("message")
    private String message;

    public String getEvent() {
        return event;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getResultsJson() {
        return resultsJson;
    }


    public String getMessage() {
        return message;
    }
}
