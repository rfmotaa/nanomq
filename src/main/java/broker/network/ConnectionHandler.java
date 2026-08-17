package broker.network;

import broker.payloads.ProducerMessage;
import broker.payloads.ConsumerMessage;
import java.io.*;
import java.net.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);

    private static final int PORT = 8080;

    public static void main(String[] args) {

        try (ServerSocket server = new ServerSocket(PORT)) {

            logger.info("Broker started on port " + PORT);

            while (true) {
                Socket client = server.accept();

                logger.info("Client connected");

                Thread thread = new Thread(() -> handleClient(client));

                thread.start();
            }

        } catch (IOException e) {
            System.out.println("Broker error: " + e.getMessage());
        }
    }

    private static void handleClient(Socket client) {
        try (client) {
            BufferedReader input = new BufferedReader(new InputStreamReader(client.getInputStream()));
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));

            String line = input.readLine();

            Gson gson = new Gson();
            JsonObject jsonMessage = JsonParser.parseString(line).getAsJsonObject();

            String action = jsonMessage.get("action").toString();

            if (action == null || (!action.equals("WRITE") && !action.equals("READ"))) {
                send(output, "Unknown command: " + action);
                return;
            }

            logger.info("Message received");

            if (action.equals("WRITE")) {
                ProducerMessage message = gson.fromJson(jsonMessage, ProducerMessage.class);

                TopicPersistence.write(message);

                send(output,"Message received");
            } else {
                ConsumerMessage message = gson.fromJson(jsonMessage, ConsumerMessage.class);

                String nextMessage = TopicPersistence.read(message);

                send(output, nextMessage);
            }

        } catch (IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
        }
    }

    private static void send(BufferedWriter output,String message) throws IOException {
        output.write(message);
        output.newLine();
        output.flush();
    }
}
