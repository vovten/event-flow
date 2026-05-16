package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.SendResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Kafka transport for sending events asynchronously.
 * <p>
 * Uses Kafka's async API with callback-based delivery. The caller receives
 * a {@link CompletableFuture} that completes when Kafka acknowledges the send.
 * <p>
 * <b>Configuration:</b>
 * Sets acks=all, idempotent producer, 3 retries, 30s delivery timeout by default.
 * Buffer memory (32MB) and max block time (5s) provide backpressure support.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class KafkaOutTransport implements OutTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutTransport.class);

    private static final long DEFAULT_BUFFER_MEMORY = 33_554_432;
    private static final long DEFAULT_MAX_BLOCK_MS = 5000;

    protected KafkaProducer<String, byte[]> producer;
    protected String topic;
    protected final EventSerializer serializer;
    protected volatile boolean closed = false;

    /**
     * Create Kafka transport with default JSON serializer.
     *
     * @param properties Kafka producer configuration
     * @param topic      Kafka topic name
     */
    public KafkaOutTransport(Properties properties, String topic) {
        this(properties, topic, new JsonEventSerializer());
    }

    /**
     * Create Kafka transport with custom serializer.
     *
     * @param properties Kafka producer configuration
     * @param topic      Kafka topic name
     * @param serializer the event serializer to use
     */
    public KafkaOutTransport(Properties properties, String topic, EventSerializer serializer) {
        Properties props = new Properties();
        props.putAll(properties);
        this.serializer = Objects.requireNonNull(serializer, "Serializer must not be null");

        props.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.putIfAbsent(ACKS_CONFIG, "all");
        props.putIfAbsent(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.putIfAbsent(RETRIES_CONFIG, "3");
        props.putIfAbsent(DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        props.putIfAbsent(BUFFER_MEMORY_CONFIG, String.valueOf(DEFAULT_BUFFER_MEMORY));
        props.putIfAbsent(MAX_BLOCK_MS_CONFIG, String.valueOf(DEFAULT_MAX_BLOCK_MS));
        props.putIfAbsent(BATCH_SIZE_CONFIG, "131072"); // 128KB
        props.putIfAbsent(LINGER_MS_CONFIG, "20");
        props.putIfAbsent(COMPRESSION_TYPE_CONFIG, "lz4");
        props.putIfAbsent(MAX_REQUEST_SIZE_CONFIG, "5242880"); // 5MB

        this.producer = new KafkaProducer<>(props);
        this.topic = Objects.requireNonNull(topic, "Topic must not be null");
    }

    /**
     * Create Kafka transport with bootstrap servers and topic.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     */
    public KafkaOutTransport(String bootstrapServers, String topic) {
        this(createDefaultProperties(bootstrapServers), topic);
    }

    /**
     * Create Kafka transport with bootstrap servers, topic and serializer.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     * @param serializer       the event serializer to use
     */
    public KafkaOutTransport(String bootstrapServers, String topic, EventSerializer serializer) {
        this(createDefaultProperties(bootstrapServers), topic, serializer);
    }

    private static Properties createDefaultProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    @Override
    public String name() {
        return "kafka";
    }

    /**
     * Send event to Kafka asynchronously. Returns immediately with a CompletableFuture
     * that completes when Kafka acknowledges the send.
     * <p>
     * Example:
     * <pre>{@code
     * transport.send(event)
     *     .thenAccept(result -> log.info("Sent to {}", result.destination()))
     *     .exceptionally(ex -> { log.error("Failed", ex); return null; });
     * }</pre>
     *
     * @param event the event to send
     * @return CompletableFuture with SendResult
     * @throws IllegalStateException if transport is closed
     */
    @Override
    public CompletableFuture<SendResult> send(Event event) {
        if (closed) {
            throw new IllegalStateException("KafkaOutTransport is already closed");
        }
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        try {
            String key = event.type().getName();
            byte[] value = serializer.serialize(event);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, createSendCallback(future, topic));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Create a Kafka callback that completes the future with success or failure result.
     *
     * @param future        the CompletableFuture to complete
     * @param topic         the Kafka topic name
     * @return Kafka Callback implementation
     */
    protected Callback createSendCallback(CompletableFuture<SendResult> future, String topic) {
        return (metadata, exception) -> {
            String destination = topic + (metadata != null ? "-p" + metadata.partition() : "");
            if (exception != null) {
                future.complete(SendResult.failure(destination, exception, buildMetaData(metadata)));
            } else {
                future.complete(SendResult.success(destination, buildMetaData(metadata)));
            }
        };
    }

    private Map<String, Object> buildMetaData(RecordMetadata metadata) {
        return metadata != null
                ? Map.of(
                "partition", metadata.partition(),
                "offset", metadata.offset(),
                "topic", metadata.topic())
                : Map.of();
    }

    /**
     * Close the Kafka producer and release all resources.
     * Safe to call multiple times.
     */
    @Override
    public void close() {
        if (!closed && producer != null) {
            closed = true;
            producer.close();
            if (log.isDebugEnabled()) {
                log.debug("KafkaOutTransport closed for topic '{}'", topic);
            }
        }
    }
}
