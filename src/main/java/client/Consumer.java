package client;

import com.google.gson.JsonObject;

import java.io.IOException;

public class Consumer {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;
    private static final String TOPIC = "demo";

    public static void main(String[] args) throws IOException {
        long offset = args.length > 0 ? Long.parseLong(args[0]) : 0;

        try (BrokerClient client = new BrokerClient(HOST, PORT)) {
            while (true) {
                JsonObject response = client.read(TOPIC, offset);

                if (!"OK".equals(string(response, "status"))) {
                    System.out.println("READ -> " + response);
                    break;
                }
                if (response.has("endOfLog") && response.get("endOfLog").getAsBoolean()) {
                    System.out.println("Reached end of log at offset " + offset);
                    break;
                }

                System.out.println("offset " + response.get("offset").getAsLong()
                        + ": " + string(response, "message"));
                offset = response.get("nextOffset").getAsLong();
            }
        }
    }

    private static String string(JsonObject obj, String key) {
        return obj.has(key) && !obj.get(key).isJsonNull() ? obj.get(key).getAsString() : null;
    }
}
