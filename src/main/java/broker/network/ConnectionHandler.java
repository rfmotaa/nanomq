package broker.network;

import broker.BrokerException;
import broker.persistence.TopicLog;
import broker.protocol.Request;
import broker.protocol.Response;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ConnectionHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);
    private static final Gson GSON = new Gson();

    private final Socket socket;
    private final TopicLog topicLog;

    public ConnectionHandler(Socket socket, TopicLog topicLog) {
        this.socket = socket;
        this.topicLog = topicLog;
    }

    @Override
    public void run() {
        try (Socket client = socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(
                     new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8))) {

            String line;
            while ((line = in.readLine()) != null) {
                if (!line.isBlank()) {
                    send(out, handle(line));
                }
            }
        } catch (IOException e) {
            logger.warn("Connection dropped: {}", e.getMessage());
        }
        logger.info("Client disconnected");
    }

    private Response handle(String line) {
        Request request;
        try {
            request = GSON.fromJson(line, Request.class);
        } catch (JsonSyntaxException e) {
            return Response.error("malformed JSON request");
        }
        if (request == null || request.getAction() == null) {
            return Response.error("missing 'action'");
        }

        try {
            return switch (request.getAction()) {
                case "CREATE" -> {
                    topicLog.create(request.getTopic());
                    yield Response.ok("created topic " + request.getTopic());
                }
                case "WRITE" -> Response.stored(topicLog.append(request.getTopic(), request.getPayload()));
                case "READ" -> {
                    long offset = request.getOffset();
                    String record = topicLog.readAt(request.getTopic(), offset);
                    yield record == null ? Response.endOfLog(offset) : Response.record(offset, record);
                }
                default -> Response.error("unknown action: " + request.getAction());
            };
        } catch (BrokerException e) {
            return Response.error(e.getMessage());
        } catch (RuntimeException e) {
            logger.error("Request failed: {}", line, e);
            return Response.error("internal broker error");
        }
    }

    private void send(BufferedWriter out, Response response) throws IOException {
        out.write(GSON.toJson(response));
        out.newLine();
        out.flush();
    }
}
