package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.publisher.RetryEventPublisher;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import com.github.vovten.eventflow.transport.OutgoingEventTransportException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Kafka outgoing transport for external event delivery.
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
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Cross-application event communication</li>
 *   <li>Event-driven microservices architecture</li>
 *   <li>Critical events that must be delivered</li>
 *   <li>Event sourcing and CQRS patterns</li>
 * </ul>
 * <p>
 * <b>Configuration example:</b>
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 * OutgoingEventTransport transport = new KafkaOutgoingEventTransport(props, "events");
 * EventChannel channel = new ExternalEventChannel(List.of(transport));
 * }</pre>
 * <p>
 * <b>Retry integration:</b>
 * For additional reliability, wrap with {@code RetryEventPublisherDecorator}:
 * <pre>{@code
 * EventPublisher publisher = new RetryEventPublisher(
 *     new ChannelEventPublisher(channels),
 *     3,                              // 3 retries
 *     Duration.ofMillis(100),         // exponential backoff
 *     2.0
 * );
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 * @see RetryEventPublisher
 */
public class KafkaOutgoingEventTransport implements OutgoingEventTransport {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    /**
     * Create Kafka transport with custom configuration.
     *
     * @param properties Kafka producer configuration
     * @param topic Kafka topic name
     */
    public KafkaOutgoingEventTransport(Properties properties, String topic) {
        Properties props = new Properties();
        props.putAll(properties);
        props.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    /**
     * Create Kafka transport with bootstrap servers and topic.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic Kafka topic name
     */
    public KafkaOutgoingEventTransport(String bootstrapServers, String topic) {
        this(createDefaultProperties(bootstrapServers), topic);
    }

    private static Properties createDefaultProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
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
     * {@link OutgoingEventTransportException} is thrown.
     * <p>
     * <b>Reliability guarantees:</b>
     * <ul>
     *   <li>Event is serialized to JSON with event type as key</li>
     *   <li>Waits for acknowledgment from all in-sync replicas</li>
     *   <li>Throws exception on any failure (network, broker, serialization)</li>
     * </ul>
     *
     * @param event the event to send
     * @throws OutgoingEventTransportException if send fails (network error, timeout, etc.)
     */
    @Override
    public void send(Event event) {
        String key = event.type().getName();
        String value = event.asJson();
        trySend(event, new ProducerRecord<>(topic, key, value));
    }

    private void trySend(Event event, ProducerRecord<String, String> record) {
        try {
            // Synchronous send with 10 second timeout
            RecordMetadata metadata = producer.send(record).get(10, TimeUnit.SECONDS);

            if (metadata == null || !metadata.hasOffset()) {
                throw new OutgoingEventTransportException(
                    String.format("Kafka returned null metadata for event %s", event.type().getSimpleName())
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutgoingEventTransportException(
                String.format("Kafka send interrupted for event %s", event.type().getSimpleName()),
                e
            );
        } catch (ExecutionException e) {
            throw new OutgoingEventTransportException(
                String.format("Failed to send event %s to Kafka topic %s: %s",
                    event.type().getSimpleName(), topic, e.getCause().getMessage()),
                e.getCause()
            );
        } catch (TimeoutException e) {
            throw new OutgoingEventTransportException(
                String.format("Timeout sending event %s to Kafka topic %s (timeout: 10s)",
                    event.type().getSimpleName(), topic),
                e
            );
        }
    }

    /**
     * Close the Kafka producer.
     */
    public void close() {
        producer.close();
    }
}
