package client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class Producer {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    public static void main(String[] args) {
        try (SocketChannel client = SocketChannel.open(new InetSocketAddress(HOST, PORT))) {

            System.out.println("Producer connected to broker.");

            send(client, "INPUT");

            String response = listen(client);

            System.out.println("Broker response: " + response);

        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void send(SocketChannel channel, String message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap((message + "\n").getBytes(StandardCharsets.UTF_8));

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static String listen(SocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        int readBytes = channel.read(buffer);

        if (readBytes == -1) {
            return null;
        }

        buffer.flip();

        return StandardCharsets.UTF_8.decode(buffer).toString().trim();
    }
}