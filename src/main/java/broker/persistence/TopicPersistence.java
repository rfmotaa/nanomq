package broker.persistence;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import broker.payloads.ProducerMessage;
import broker.payloads.ConsumerMessage;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicPersistence {
    private static final Map<String, Path> topics = new HashMap<>();

    private static final Map<Path, String> offsets = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(TopicPersistence.class);

    // TODO: create a new error class for the project itself, that will also be used for TCP communications
    public static void write(ProducerMessage payload) {

    }

    public static String read(ConsumerMessage payload) {
        return "";
    }

    public static void create() {

    }

    private static void creteTopic(String topicName) {
        if (topics.containsKey(topicName)) {
            String errorMessage = "A topic with name " + topicName + " already exists.";
            logger.error(errorMessage);
            throw new "ERROR: " + errorMessage;
        }

        Path newTopicPath = Paths.get("/topic/" + topicName);
        Path newTopicIndex = Paths.get("/topic/" + topicName + "/.index");
        Path newTopicSegments = Paths.get("/topic/" + topicName + "/segments");
        Path newTopicLog = Paths.get("/topic/" + topicName + "/segments" + "/.log1");

        try {
            Files.createDirectories(newTopicPath);
            Files.createFile(newTopicIndex);
            Files.createDirectories(newTopicSegments);
            Files.createFile(newTopicLog);

            topics.put(topicName, newTopicPath);
        } catch (IOException e) {
            throw new Error;
        }

    }
}
