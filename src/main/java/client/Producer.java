package client;

import com.google.gson.JsonObject;

import java.io.IOException;

public class Producer {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;
    private static final String TOPIC = "demo";

    public static void main(String[] args) throws IOException {
        try (BrokerClient client = new BrokerClient(HOST, PORT)) {
            System.out.println("CREATE -> " + client.create(TOPIC));

            for (int i = 1; i <= 5; i++) {
                JsonObject ack = client.write(TOPIC, "hello #" + i);
                System.out.println("WRITE  -> " + ack);
            }
        }
    }
}
