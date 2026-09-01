package broker.persistence;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import broker.payloads.ProducerMessage;
import broker.payloads.ConsumerMessage;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicPersistence {
    // Topic-name -> Path for its current log
    private static final Map<String, Path> topics = new HashMap<>();
    // Path from topic -> offset in that topic
    private static final Map<Path, HashSet<String>> offsets = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(TopicPersistence.class);

    // TODO: create a new error class for the project itself, that will also be used for TCP communications
    public static void write(ProducerMessage payload) {
        if (!topics.containsKey(payload.getTopic())) {
            throw new Error;
        }

        Path topicPath = topics.get(payload.getTopic());

        writeMessageOnTopic(topicPath, payload.getPayload());
    }

    public static String read(ConsumerMessage payload) {
        if (!topics.containsKey(payload.getTopic())) {
            throw new Error;
        }

        Path topicPath = topics.get(payload.getTopic());

        if(!offsets.get(topicPath).contains(payload.getOffsetId())) {
            throw new Error;
        }

        String message = getNextMessageForOffest(topicPath, payload.getOffsetId());
    }

    private static String getNextMessageForOffest(Path topicLocation, String offsetId) {
        return "";
    }

    private static void writeMessageOnTopic(Path topicLocation, String message) {

    }

    private static String createOffsetInTopic(String topic) {
        if (!topics.containsKey(topic)) {
            throw new Error;
        }

        Path topicPath = topics.get(topic);
        String newOffsetId = generateNewRandomOffestId();
        offsets.computeIfAbsent(topicPath, k -> new HashSet<>()).add(newOffsetId);

        return newOffsetId;
    }

    private static void creteTopic(String topicName) {
        if (topics.containsKey(topicName)) {
            String errorMessage = "A topic with name " + topicName + " already exists.";
            logger.error(errorMessage);
            throw new Error;
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

    private static String generateNewRandomOffestId() {
        final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        final int SIZE = 16;
        SecureRandom random = new SecureRandom();
        StringBuilder sb = new StringBuilder(SIZE);

        for (int i = 0; i < SIZE; i++) {
            int idx = random.nextInt(CHARS.length());
            sb.append(CHARS.charAt(idx));
        }

        return sb.toString();
    }
}
