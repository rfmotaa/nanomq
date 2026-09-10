package broker;

import client.BrokerClient;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerEndToEndTest {

    private BrokerServer broker;

    @BeforeEach
    void startBroker(@TempDir Path dataDir) throws IOException {
        broker = new BrokerServer(0, dataDir); // port 0 -> OS picks a free port
        broker.start();
    }

    @AfterEach
    void stopBroker() {
        broker.close();
    }

    @Test
    void producedRecordsAreConsumedInOrder() throws IOException {
        try (BrokerClient client = new BrokerClient("localhost", broker.port())) {
            assertEquals("OK", client.create("orders").get("status").getAsString());

            for (int i = 0; i < 3; i++) {
                JsonObject ack = client.write("orders", "order-" + i);
                assertEquals("OK", ack.get("status").getAsString());
                assertEquals(i, ack.get("offset").getAsLong());
            }

            long offset = 0;
            for (int i = 0; i < 3; i++) {
                JsonObject record = client.read("orders", offset);
                assertEquals("order-" + i, record.get("message").getAsString());
                assertFalse(record.get("endOfLog").getAsBoolean());
                offset = record.get("nextOffset").getAsLong();
            }

            JsonObject end = client.read("orders", offset);
            assertTrue(end.get("endOfLog").getAsBoolean());
        }
    }

    @Test
    void writingToAnUnknownTopicReturnsError() throws IOException {
        try (BrokerClient client = new BrokerClient("localhost", broker.port())) {
            JsonObject response = client.write("ghost", "x");
            assertEquals("ERROR", response.get("status").getAsString());
        }
    }

    @Test
    void creatingTheSameTopicTwiceReturnsError() throws IOException {
        try (BrokerClient client = new BrokerClient("localhost", broker.port())) {
            assertEquals("OK", client.create("dup").get("status").getAsString());
            assertEquals("ERROR", client.create("dup").get("status").getAsString());
        }
    }

    @Test
    void recordsSurviveABrokerRestart(@TempDir Path dataDir) throws IOException {
        broker.close();
        broker = new BrokerServer(0, dataDir);
        broker.start();
        try (BrokerClient client = new BrokerClient("localhost", broker.port())) {
            client.create("persist");
            client.write("persist", "kept");
        }

        broker.close();
        broker = new BrokerServer(0, dataDir);
        broker.start();
        try (BrokerClient client = new BrokerClient("localhost", broker.port())) {
            JsonObject record = client.read("persist", 0);
            assertEquals("kept", record.get("message").getAsString());

            JsonObject ack = client.write("persist", "appended");
            assertEquals(1, ack.get("offset").getAsLong());
        }
    }
}
