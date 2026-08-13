package broker.network;

import java.io.*;
import java.net.*;
import java.util.ArrayList;

public class ConnectionHandler {

    private static final int PORT = 8080;

    private static final ArrayList<String> data = new ArrayList<>();

    private static int counter = 0;

    public static void main(String[] args) {

        try (ServerSocket server = new ServerSocket(PORT)) {

            System.out.println("Broker started on port " + PORT);

            while (true) {
                Socket client = server.accept();

                System.out.println("Client connected: " + client.getInetAddress().getHostAddress());

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

            String message = input.readLine();

            if (message == null) {
                return;
            }

            System.out.println("Message received: " + message);

            if (message.equals("INPUT")) {
                String item;

                synchronized (data) {
                    counter++;
                    item = "DATA - " + counter;
                    data.add(item);
                }

                send(output,"Message received: " + counter);
            }

            else if (message.equals("OUTPUT")) {
                String last;

                synchronized (data) {
                    if (data.isEmpty()) {
                        send(output, "Queue empty");
                        return;
                    }

                    last = data.removeLast();
                }

                send(output, "Message consumed: " + last);
            }

            else {
                send(output, "Unknown command: " + message);
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
