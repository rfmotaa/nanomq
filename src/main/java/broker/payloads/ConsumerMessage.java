package broker.payloads;

public class ConsumerMessage {
    public String action;
    public String topic;
    public String memberId;

    ConsumerMessage(String action, String topic, String memberId) {
        this.action = action;
        this.topic = topic;
        this.memberId = memberId;
    }

    public String toString() {
        return "Message\nTopic: " + topic + " - Member Id: " + memberId;
    }

    public String getAction() { return this.action; }
    public String getTopic() { return this.topic; }
    public String getMemberId() { return this.memberId; }
}
