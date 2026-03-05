package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * Kafka transport for external event delivery.
 * <p>
 * This transport sends events to an Apache Kafka topic.
 * Events are serialized to JSON before being sent.
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Cross-application event communication</li>
 *   <li>Event-driven microservices architecture</li>
 *   <li>High-throughput event streaming</li>
 *   <li>Event sourcing and CQRS patterns</li>
 * </ul>
 * <p>
 * <b>Configuration example:</b>
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 * EventTransport transport = new KafkaEventTransport(props, "events");
 * EventChannel channel = new ExternalEventChannel(List.of(transport));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class KafkaEventTransport implements EventTransport {
    
    private final KafkaProducer<String, String> producer;
    private final String topic;
    
    /**
     * Create Kafka transport with custom configuration.
     *
     * @param properties Kafka producer configuration
     * @param topic Kafka topic name
     */
    public KafkaEventTransport(Properties properties, String topic) {
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
    public KafkaEventTransport(String bootstrapServers, String topic) {
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

    @Override
    public void send(Event event) {
        var record = new ProducerRecord<String, String>(topic, event.asJson());
        producer.send(record);
    }

    /**
     * Close the Kafka producer.
     */
    public void close() {
        producer.close();
    }
}
