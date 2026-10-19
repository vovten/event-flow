package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
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
 * Abstract base class for Kafka transport implementations.
 * <p>
 * Provides common functionality for sending events to Kafka topics with a configurable
 * {@link EventKeyResolver}. Subclasses may override {@link #dispatch(Event, String, byte[])}
 * to provide specific delivery semantics (e.g., broadcasting to all partitions).
 * <p>
 * <b>Default configuration:</b>
 * <ul>
 *   <li>Acks: all</li>
 *   <li>Idempotent producer: enabled</li>
 *   <li>Retries: 3</li>
 *   <li>Delivery timeout: 35s</li>
 *   <li>Buffer memory: 64MB</li>
 *   <li>Max block time: 5s (backpressure)</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 * @see KafkaOutTransport
 * @see BroadcastKafkaOutTransport
 * @see EventKeyResolver
 */
public abstract class AbstractKafkaOutTransport implements OutTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaOutTransport.class);

    private static final long DEFAULT_BUFFER_MEMORY = 67_108_864;
    private static final long DEFAULT_MAX_BLOCK_MS = 5000;

    /**
     * Key resolver for determining the Kafka record key and thus partition routing.
     */
    protected final EventKeyResolver keyResolver;

    /**
     * Event serializer for converting events to byte arrays.
     */
    protected final EventSerializer serializer;

    /**
     * Kafka producer instance.
     */
    protected KafkaProducer<String, byte[]> producer;

    /**
     * Target Kafka topic name.
     */
    protected String topic;

    /**
     * Flag indicating if the transport has been closed.
     */
    protected volatile boolean closed = false;

    /**
     * Creates a Kafka transport with all configuration parameters.
     *
     * @param properties  Kafka producer configuration properties
     * @param topic       target Kafka topic name
     * @param serializer  event serializer
     * @param keyResolver key resolver strategy
     * @throws NullPointerException if any parameter is null
     */
    protected AbstractKafkaOutTransport(Properties properties, String topic, EventSerializer serializer,
                                        EventKeyResolver keyResolver) {
        this.topic = Objects.requireNonNull(topic, "Topic must not be null");
        this.serializer = Objects.requireNonNull(serializer, "Serializer must not be null");
        this.keyResolver = Objects.requireNonNull(keyResolver, "EventKeyResolver must not be null");

        Properties props = new Properties();
        props.putAll(Objects.requireNonNull(properties, "Properties must not be null"));
        props.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.putIfAbsent(ACKS_CONFIG, "all");
        props.putIfAbsent(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.putIfAbsent(RETRIES_CONFIG, "3");
        props.putIfAbsent(DELIVERY_TIMEOUT_MS_CONFIG, "35000");
        props.putIfAbsent(BUFFER_MEMORY_CONFIG, String.valueOf(DEFAULT_BUFFER_MEMORY));
        props.putIfAbsent(MAX_BLOCK_MS_CONFIG, String.valueOf(DEFAULT_MAX_BLOCK_MS));
        props.putIfAbsent(BATCH_SIZE_CONFIG, "131072"); // 128KB
        props.putIfAbsent(LINGER_MS_CONFIG, "10");
        props.putIfAbsent(COMPRESSION_TYPE_CONFIG, "snappy");
        props.putIfAbsent(MAX_REQUEST_SIZE_CONFIG, "5242880"); // 5MB

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public String name() {
        return "kafka";
    }

    /**
     * Send event to Kafka asynchronously. Returns immediately with a CompletableFuture
     * that completes when Kafka acknowledges the send.
     * <p>
     * This is a template method: it checks the transport state, resolves the record
     * key via the configured {@link EventKeyResolver}, serializes the event and
     * delegates the actual delivery to {@link #dispatch(Event, String, byte[])}.
     * Any failure — key resolution, serialization or dispatch — completes the
     * returned future exceptionally, so subclasses share one error contract.
     *
     * @param event the event to send
     * @return CompletableFuture with SendResult
     * @throws IllegalStateException if transport is closed
     */
    @Override
    public final CompletableFuture<SendResult> send(Event event) {
        if (closed) {
            throw new IllegalStateException("%s is already closed".formatted(getClass().getSimpleName()));
        }
        try {
            String key = keyResolver.resolveKey(event);
            byte[] value = serializer.serialize(event);
            return dispatch(event, key, value);
        } catch (Exception e) {
            CompletableFuture<SendResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    /**
     * Dispatch the serialized event to Kafka. The base implementation sends a single
     * record without an explicit partition, letting Kafka route it by the resolved key
     * (round-robin when the key is {@code null}).
     * <p>
     * Subclasses may override to provide specific delivery semantics
     * (e.g., broadcasting to all partitions). Exceptions thrown here are captured by
     * {@link #send(Event)} and surfaced through the returned future.
     *
     * @param event the event being sent (for error reporting)
     * @param key   the Kafka record key resolved by the {@link EventKeyResolver}, may be null
     * @param value the serialized event payload
     * @return CompletableFuture with SendResult
     */
    protected CompletableFuture<SendResult> dispatch(Event event, String key, byte[] value) {
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, createSendCallback(future));
        return future;
    }

    /**
     * Create a Kafka callback that completes the future with success or failure result.
     * The destination is annotated with the partition taken from the record metadata.
     *
     * @param future the CompletableFuture to complete
     * @return Kafka Callback implementation
     */
    protected Callback createSendCallback(CompletableFuture<SendResult> future) {
        return (metadata, exception) -> {
            String destination = name() + "-" + topic + (metadata != null ? "-p" + metadata.partition() : "");
            if (exception != null) {
                future.complete(SendResult.failure(destination, exception, buildMetaData(metadata)));
            } else {
                future.complete(SendResult.success(destination, buildMetaData(metadata)));
            }
        };
    }

    /**
     * Build metadata map from Kafka record metadata.
     *
     * @param metadata the Kafka record metadata
     * @return metadata map with partition, offset, and topic
     */
    protected Map<String, Object> buildMetaData(RecordMetadata metadata) {
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