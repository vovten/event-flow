package io.github.vovten.eventflow.transport.incoming.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Kafka consumer configuration helpers for the inbound event transport.
 * <p>
 * Centralizes the at-least-once defaults so the two property paths (explicit
 * defaults and user overrides) cannot drift apart.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class KafkaConsumerProperties {

    private static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";

    private KafkaConsumerProperties() {
        // Utility class
    }

    /**
     * Builds the default consumer properties for at-least-once delivery:
     * auto-commit disabled and {@code auto.offset.reset=earliest}.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId          consumer group ID
     * @return consumer properties
     */
    public static Properties defaults(String bootstrapServers, String groupId) {
        requireGroupId(groupId);
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        applyAtLeastOnceDefaults(props);
        return props;
    }

    /**
     * Completes the user-provided properties with the required deserializers,
     * the group id and the at-least-once defaults.
     *
     * @param overrides user-provided properties (not mutated)
     * @param groupId   consumer group ID
     * @return a new completed properties instance
     */
    public static Properties withOverrides(Properties overrides, String groupId) {
        Properties props = new Properties();
        props.putAll(overrides);
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        requireGroupId(props.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        applyAtLeastOnceDefaults(props);
        return props;
    }

    /**
     * Applies the at-least-once defaults for values the caller did not set.
     * <p>
     * User-provided values always win: explicitly setting
     * {@code enable.auto.commit=true} or a different {@code auto.offset.reset}
     * disables the corresponding at-least-once guarantee, and the transport will
     * rely on the caller's offset management.
     *
     * @param props consumer properties to complete
     */
    public static void applyAtLeastOnceDefaults(Properties props) {
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST);
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.putIfAbsent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "524288");
        props.putIfAbsent(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
    }

    private static void requireGroupId(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("consumer-group must not be blank");
        }
    }

    /**
     * Splits a comma-separated topic list, trimming each name and ignoring
     * blank entries.
     * <p>
     * The result may be empty for a blank input; the transport constructors
     * reject an empty topic list.
     *
     * @param topics comma-separated topics
     * @return list of topic names
     * @throws IllegalArgumentException if {@code topics} is null
     */
    public static List<String> parseTopics(String topics) {
        if (topics == null) {
            throw new IllegalArgumentException("Topic list must not be null");
        }
        return Arrays.stream(topics.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .toList();
    }
}
