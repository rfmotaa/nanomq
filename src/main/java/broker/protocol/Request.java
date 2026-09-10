package broker.protocol;

public class Request {
    private String action;
    private String topic;
    private String payload;   // WRITE only
    private Long offset;       // READ only
    private String clientId;

    public String getAction() {
        return action;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public long getOffset() {
        return offset == null ? 0L : offset;
    }

    public String getClientId() {
        return clientId;
    }
}
