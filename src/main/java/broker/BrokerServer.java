package broker;

import broker.network.ConnectionHandler;
import broker.persistence.TopicLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    private final ServerSocket server;
    private final TopicLog topicLog;
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final Thread acceptLoop = new Thread(this::acceptConnections, "broker-accept");

    public BrokerServer(int port, Path dataDir) throws IOException {
        this.topicLog = new TopicLog(dataDir);
        this.server = new ServerSocket(port);
    }

    public void start() {
        acceptLoop.start();
        logger.info("Broker listening on port {}", port());
    }

    public int port() {
        return server.getLocalPort();
    }

    private void acceptConnections() {
        while (!server.isClosed()) {
            try {
                Socket client = server.accept();
                logger.info("Client connected: {}", client.getRemoteSocketAddress());
                workers.submit(new ConnectionHandler(client, topicLog));
            } catch (IOException e) {
                if (!server.isClosed()) {
                    logger.warn("Failed to accept connection: {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() {
        workers.shutdownNow();
        try {
            server.close();
        } catch (IOException e) {
            logger.warn("Error while closing server socket: {}", e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.getInteger("broker.port", 8080);
        Path dataDir = Paths.get(System.getProperty("broker.dataDir", "data"));

        BrokerServer broker = new BrokerServer(port, dataDir);
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close, "broker-shutdown"));
        broker.start();
    }
}
