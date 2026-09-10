package broker.persistence;

import broker.BrokerException;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class TopicLog {
    private static final Logger logger = LoggerFactory.getLogger(TopicLog.class);
    private static final Gson GSON = new Gson();

    private final Path baseDir;
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> endOffsets = new ConcurrentHashMap<>();

    public TopicLog(Path baseDir) {
        this.baseDir = baseDir;
        try {
            Files.createDirectories(baseDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not create data directory " + baseDir, e);
        }
        logger.info("Storing topics under {}", baseDir.toAbsolutePath());
    }

    public void create(String topic) {
        Path path = pathFor(topic);
        synchronized (lockFor(topic)) {
            if (Files.exists(path)) {
                throw new BrokerException("topic already exists: " + topic);
            }
            try {
                Files.createFile(path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            endOffsets.put(topic, 0L);
        }
        logger.info("Created topic {}", topic);
    }

    public long append(String topic, String payload) {
        if (payload == null) {
            throw new BrokerException("WRITE requires a payload");
        }
        Path path = requireTopic(topic);
        synchronized (lockFor(topic)) {
            long offset = endOffset(topic, path);
            try {
                Files.writeString(path, GSON.toJson(payload) + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            endOffsets.put(topic, offset + 1);
            return offset;
        }
    }

    public String readAt(String topic, long offset) {
        if (offset < 0) {
            throw new BrokerException("offset must not be negative: " + offset);
        }
        Path path = requireTopic(topic);
        List<String> lines;
        try {
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (offset >= lines.size()) {
            return null;
        }
        return GSON.fromJson(lines.get((int) offset), String.class);
    }

    private long endOffset(String topic, Path path) {
        Long cached = endOffsets.get(topic);
        if (cached != null) {
            return cached;
        }
        long count;
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            count = lines.count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        endOffsets.put(topic, count);
        return count;
    }

    private Path requireTopic(String topic) {
        Path path = pathFor(topic);
        if (!Files.exists(path)) {
            throw new BrokerException("unknown topic: " + topic);
        }
        return path;
    }

    private Path pathFor(String topic) {
        if (topic == null || !topic.matches("[A-Za-z0-9._-]{1,255}")) {
            throw new BrokerException("invalid topic name: " + topic);
        }
        return baseDir.resolve(topic + ".log");
    }

    private Object lockFor(String topic) {
        return locks.computeIfAbsent(topic, k -> new Object());
    }
}
