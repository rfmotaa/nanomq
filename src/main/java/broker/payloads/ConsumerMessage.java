package broker.payloads;

public class ConsumerMessage {
    private final String action;
    private final String topic;
    private final String offsetId;
    private final String memberId;

    ConsumerMessage(String action, String topic, String memberId, String offsetId) {
        this.action = action;
        this.topic = topic;
        this.memberId = memberId;
        this.offsetId = offsetId;
    }

    public String toString() {
        return "Message\nTopic: " + topic + " - Member Id: " + memberId;
    }

    public String getAction() { return this.action; }
    public String getTopic() { return this.topic; }
    public String getMemberId() { return this.memberId; }
    public String getOffsetId() { return this.offsetId; }
}
