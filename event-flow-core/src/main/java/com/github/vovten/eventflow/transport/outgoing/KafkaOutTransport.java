package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.publisher.RetryEventPublisher;
import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.TransportException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Kafka publisher transport for external event delivery.
 * <p>
 * This transport implements {@link AutoCloseable} to ensure proper resource cleanup.
 * Always close the transport when it's no longer needed:
 * <pre>{@code
 * try (KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "events")) {
 *     transport.send(event);
 * }
 * }</pre>
 * <p>
 * This transport sends events to an Apache Kafka topic using <b>synchronous</b> delivery.
 * Each event is sent and the transport waits for acknowledgment from Kafka brokers.
 * This ensures reliable delivery — if the send fails, an exception is thrown immediately.
 * <p>
 * <b>Reliability features:</b>
 * <ul>
 *   <li>Synchronous send with timeout (10 seconds)</li>
 *   <li>Waits for acknowledgment from all in-sync replicas (acks=all)</li>
 *   <li>Idempotent producer enabled (no duplicates)</li>
 *   <li>Automatic retries on transient failures (3 retries)</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 * @see RetryEventPublisher
 * @see EventSerializer
 */
public class KafkaOutTransport implements OutTransport, AutoCloseable {

    protected KafkaProducer<String, byte[]> producer;
    protected String topic;
    protected final EventSerializer serializer;

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
        this.serializer = serializer;

        props.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

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
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.setProperty(ACKS_CONFIG, "all");
        props.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(RETRIES_CONFIG, "3");
        props.setProperty(DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        return props;
    }

    @Override
    public String name() {
        return "kafka";
    }

    /**
     * Send event to Kafka topic synchronously.
     * <p>
     * This method sends the event and waits for acknowledgment from Kafka brokers.
     * The send operation has a timeout of 10 seconds. If the send fails, an
     * {@link TransportException} is thrown.
     * <p>
     * <b>Reliability guarantees:</b>
     * <ul>
     *   <li>Event is serialized with format header (magic byte)</li>
     *   <li>Waits for acknowledgment from all in-sync replicas</li>
     *   <li>Throws exception on any failure (network, broker, serialization)</li>
     * </ul>
     *
     * @param event the event to send
     * @throws TransportException if send fails (network error, timeout, etc.)
     */
    @Override
    public void send(Event event) {
        String key = event.type().getName();
        byte[] value = serializer.serialize(event);
        trySend(event, new ProducerRecord<>(topic, key, value));
    }

    /**
     * Send a producer record with synchronous delivery.
     * <p>
     * This method is protected to allow subclasses to customize send behavior.
     *
     * @param event  the event being sent
     * @param record the producer record to send
     * @throws TransportException if send fails
     */
    protected void trySend(Event event, ProducerRecord<String, byte[]> record) {
        try {
            // Synchronous send with 10 second timeout
            RecordMetadata metadata = producer.send(record).get(10, TimeUnit.SECONDS);

            if (metadata == null || !metadata.hasOffset()) {
                throw new TransportException(
                        String.format("Kafka returned null metadata for event %s", event.type().getSimpleName())
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransportException(
                    String.format("Kafka send interrupted for event %s", event.type().getSimpleName()),
                    e
            );
        } catch (ExecutionException e) {
            throw new TransportException(
                    String.format("Failed to send event %s to Kafka topic %s: %s",
                            event.type().getSimpleName(), topic, e.getCause().getMessage()),
                    e.getCause()
            );
        } catch (TimeoutException e) {
            throw new TransportException(
                    String.format("Timeout sending event %s to Kafka topic %s (timeout: 10s)",
                            event.type().getSimpleName(), topic),
                    e
            );
        }
    }

    /**
     * Close the Kafka producer and release all resources.
     * <p>
     * This method is idempotent and safe to call multiple times.
     * After closing, the transport cannot be used again.
     */
    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
