package com.github.vovten.eventflow.transport.incoming;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.EventSerializerFactory;
import com.github.vovten.eventflow.transport.InTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka incoming transport for receiving external events.
 * <p>
 * This transport listens to one or more Kafka topics and delivers events
 * to the registered consumer. It uses synchronous polling with configurable
 * timeout and supports graceful shutdown.
 * <p>
 * Automatically detects event serialization format:
 * - Old JSON format (starts with '{' = 0x7B)
 * - New JSON format (magic byte 0x01)
 * - MessagePack format (magic byte 0x02)
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Cross-application event communication</li>
 *   <li>Event-driven microservices architecture</li>
 *   <li>Receiving events from external systems</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-06
 * @see EventSerializerFactory
 */
@Slf4j
public class KafkaInTransport implements InTransport {

    private final Consumer<String, byte[]> kafkaConsumer;
    private final List<String> topics;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final EventSerializerFactory serializerFactory;

    /**
     * Poll timeout in milliseconds.
     */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    /**
     * Create Kafka transport with bootstrap servers, topics, and group ID.
     * Uses default EventSerializerFactory with JSON and MessagePack serializers.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topicsConfig     comma-separated list of topics to subscribe to
     * @param groupId          consumer group ID
     */
    public KafkaInTransport(String bootstrapServers, String topicsConfig, String groupId) {
        this(createConsumer(bootstrapServers, groupId), parseTopics(topicsConfig),
                Executors.newSingleThreadExecutor(), new EventSerializerFactory());
    }

    /**
     * Create Kafka transport with custom properties, topics, and group ID.
     * Uses default EventSerializerFactory with JSON and MessagePack serializers.
     *
     * @param properties   Kafka consumer properties
     * @param topicsConfig comma-separated list of topics to subscribe to
     * @param groupId      consumer group ID
     */
    public KafkaInTransport(Properties properties, String topicsConfig, String groupId) {
        this(createConsumer(properties, groupId), parseTopics(topicsConfig),
                Executors.newSingleThreadExecutor(), new EventSerializerFactory());
    }

    /**
     * Create Kafka transport with custom consumer (for tests).
     * Uses default EventSerializerFactory with JSON and MessagePack serializers.
     *
     * @param kafkaConsumer Kafka consumer instance
     * @param topics        list of topics to subscribe to
     */
    public KafkaInTransport(Consumer<String, byte[]> kafkaConsumer, List<String> topics) {
        this(kafkaConsumer, topics, Executors.newSingleThreadExecutor(), new EventSerializerFactory());
    }

    /**
     * Create Kafka transport with custom consumer and serializer factory.
     *
     * @param kafkaConsumer      Kafka consumer instance
     * @param topics             list of topics to subscribe to
     * @param serializerFactory  serializer factory for event deserialization
     */
    public KafkaInTransport(Consumer<String, byte[]> kafkaConsumer,
                            List<String> topics,
                            EventSerializerFactory serializerFactory) {
        this(kafkaConsumer, topics, Executors.newSingleThreadExecutor(), serializerFactory);
    }

    /**
     * Create Kafka transport with custom consumer, executor, and serializer factory.
     *
     * @param kafkaConsumer      Kafka consumer instance
     * @param topics             list of topics to subscribe to
     * @param executorService    executor service for running the consumer loop
     * @param serializerFactory  serializer factory for event deserialization
     */
    public KafkaInTransport(Consumer<String, byte[]> kafkaConsumer,
                            List<String> topics,
                            ExecutorService executorService,
                            EventSerializerFactory serializerFactory) {
        this.topics = topics;
        this.kafkaConsumer = kafkaConsumer;
        this.executorService = executorService;
        this.serializerFactory = serializerFactory;
    }

    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public void start(java.util.function.Consumer<Event> eventConsumer) {
        if (running.compareAndSet(false, true)) {
            executorService.execute(() -> consumeLoop(eventConsumer));
            log.info("KafkaDispatcherTransport started, listening to topics: {}", topics);
        } else {
            log.warn("KafkaDispatcherTransport is already running");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping KafkaDispatcherTransport...");
            if (kafkaConsumer != null) {
                tryStopConsumer();
            }
            executorService.shutdownNow();
            log.info("KafkaDispatcherTransport stopped");
        }
    }

    private void consumeLoop(java.util.function.Consumer<Event> eventConsumer) {
        try {
            kafkaConsumer.subscribe(topics);
            while (running.get()) {
                tryPollRecords(kafkaConsumer, eventConsumer);
            }
        } catch (Exception e) {
            if (running.get()) {
                log.error("KafkaDispatcherTransport loop error", e);
            }
        }
    }

    private void tryPollRecords(Consumer<String, byte[]> kafkaConsumer,
                                java.util.function.Consumer<Event> eventConsumer) {
        try {
            for (ConsumerRecord<String, byte[]> record : kafkaConsumer.poll(POLL_TIMEOUT)) {
                tryDeliverEvent(record, eventConsumer);
            }
        } catch (Exception e) {
            if (running.get()) {
                log.error("Error during Kafka message processing", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void tryDeliverEvent(ConsumerRecord<String, byte[]> record,
                                 java.util.function.Consumer<Event> eventConsumer) {
        try {
            byte[] data = record.value();
            EventSerializer serializer = serializerFactory.getByData(data);
            Event event = serializer.deserialize(data, Event.class);
            eventConsumer.accept(event);
            log.debug("Event delivered from Kafka topic: {}, key: {}, event type: {}",
                    record.topic(), record.key(), event.type().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to deliver event from topic: {}, key: {}", record.topic(), record.key(), e);
        }
    }

    private void tryStopConsumer() {
        try {
            kafkaConsumer.wakeup();
        } catch (Exception e) {
            log.warn("Error waking up consumer", e);
        }
        try {
            kafkaConsumer.close();
        } catch (Exception e) {
            log.warn("Error closing consumer", e);
        }
    }

    private static Consumer<String, byte[]> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        return new KafkaConsumer<>(props);
    }

    private static Consumer<String, byte[]> createConsumer(Properties properties, String groupId) {
        Properties props = new Properties();
        props.putAll(properties);
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private static List<String> parseTopics(String topicsConfig) {
        return List.of(topicsConfig.split(","));
    }
}
