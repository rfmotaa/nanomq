package client;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class BrokerClient implements AutoCloseable {
    private static final Gson GSON = new Gson();

    private final Socket socket;
    private final BufferedReader in;
    private final BufferedWriter out;

    public BrokerClient(String host, int port) throws IOException {
        this.socket = new Socket();
        this.socket.connect(new InetSocketAddress(host, port));
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
    }

    public JsonObject create(String topic) throws IOException {
        JsonObject req = new JsonObject();
        req.addProperty("action", "CREATE");
        req.addProperty("topic", topic);
        return call(req);
    }

    public JsonObject write(String topic, String payload) throws IOException {
        JsonObject req = new JsonObject();
        req.addProperty("action", "WRITE");
        req.addProperty("topic", topic);
        req.addProperty("payload", payload);
        return call(req);
    }

    public JsonObject read(String topic, long offset) throws IOException {
        JsonObject req = new JsonObject();
        req.addProperty("action", "READ");
        req.addProperty("topic", topic);
        req.addProperty("offset", offset);
        return call(req);
    }

    private JsonObject call(JsonObject request) throws IOException {
        out.write(GSON.toJson(request));
        out.newLine();
        out.flush();

        String response = in.readLine();
        if (response == null) {
            throw new IOException("broker closed the connection");
        }
        return GSON.fromJson(response, JsonObject.class);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
