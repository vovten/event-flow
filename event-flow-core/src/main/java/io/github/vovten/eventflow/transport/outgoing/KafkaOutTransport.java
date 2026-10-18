package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Kafka transport for sending events asynchronously.
 * <p>
 * Uses Kafka's async API with callback-based delivery. The caller receives
 * a {@link java.util.concurrent.CompletableFuture} that completes when Kafka acknowledges the send.
 * <p>
 * The Kafka record key is resolved by an {@link EventKeyResolver}. By default the
 * {@code none} strategy is used (no key, round-robin partition assignment), which
 * preserves the legacy behavior. Configure a different strategy to control which
 * partition receives the event, e.g.:
 * <pre>{@code
 * KafkaOutTransport transport = new KafkaOutTransport(
 *     "localhost:9092", "events", new JsonEventSerializer(),
 *     EventKeyResolver.processId()
 * );
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class KafkaOutTransport extends AbstractKafkaOutTransport {

    /**
     * Create Kafka transport with default JSON serializer and no key (round-robin).
     *
     * @param properties Kafka producer configuration
     * @param topic      Kafka topic name
     */
    public KafkaOutTransport(Properties properties, String topic) {
        this(properties, topic, new JsonEventSerializer(), EventKeyResolver.none());
    }

    /**
     * Create Kafka transport with custom serializer and no key (round-robin).
     *
     * @param properties Kafka producer configuration
     * @param topic      Kafka topic name
     * @param serializer the event serializer to use
     */
    public KafkaOutTransport(Properties properties, String topic, EventSerializer serializer) {
        this(properties, topic, serializer, EventKeyResolver.none());
    }

    /**
     * Create Kafka transport with custom serializer and key resolver.
     *
     * @param properties  Kafka producer configuration
     * @param topic       Kafka topic name
     * @param serializer  the event serializer to use
     * @param keyResolver key resolver strategy
     */
    public KafkaOutTransport(Properties properties, String topic, EventSerializer serializer,
                             EventKeyResolver keyResolver) {
        super(properties, topic, serializer, keyResolver);
    }

    /**
     * Create Kafka transport with bootstrap servers and topic.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     */
    public KafkaOutTransport(String bootstrapServers, String topic) {
        this(createDefaultProperties(bootstrapServers), topic, new JsonEventSerializer(), EventKeyResolver.none());
    }

    /**
     * Create Kafka transport with bootstrap servers, topic and serializer.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     * @param serializer       the event serializer to use
     */
    public KafkaOutTransport(String bootstrapServers, String topic, EventSerializer serializer) {
        this(createDefaultProperties(bootstrapServers), topic, serializer, EventKeyResolver.none());
    }

    /**
     * Create Kafka transport with bootstrap servers, topic, serializer and key resolver.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     * @param serializer       the event serializer to use
     * @param keyResolver      key resolver strategy
     */
    public KafkaOutTransport(String bootstrapServers, String topic, EventSerializer serializer,
                             EventKeyResolver keyResolver) {
        super(createDefaultProperties(bootstrapServers), topic, serializer, keyResolver);
    }

    private static Properties createDefaultProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }
}