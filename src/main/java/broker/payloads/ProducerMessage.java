package broker.payloads;

import java.nio.charset.StandardCharsets;

public class ProducerMessage {
    public String action;
    public String topic;
    public String payload;
    public String memberId;

    ProducerMessage(String action, String topic, String payload, String memberId) {
        this.action = action;
        this.topic = topic;
        this.payload = payload;
        this.memberId = memberId;
    }

    public String toString() {
        return "Message\nTopic: " + topic + " - Member Id: " + memberId + " - Payload size: " + payload.getBytes(StandardCharsets.UTF_8).length;
    }

    public String getAction() { return this.action; }
    public String getTopic() { return this.topic; }
    public String getPayload() { return this.payload; }
    public String getMemberId() { return this.memberId; }
}
