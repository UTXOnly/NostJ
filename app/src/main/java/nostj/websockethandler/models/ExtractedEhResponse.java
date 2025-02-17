package nostj.websockethandler.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map; 

public class ExtractedEhResponse {
    @JsonProperty("event")
    private String event;

    @JsonProperty("subscription_id")
    private String subscriptionId;
    
    @JsonProperty("message")
    private String message;

    @JsonProperty("results_json")
    private String resultsString; 

    public String getEvent() {
        return event;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getResultsString() { 
        return resultsString;
    }

    public String getMessage() {
        return message;
    }
}
