package com.github.vovten.eventflow.transport.incoming;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.util.EventUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
 * <b>When to use:</b>
 * <ul>
 *   <li>Cross-application event communication</li>
 *   <li>Event-driven microservices architecture</li>
 *   <li>Receiving events from external systems</li>
 * </ul>
 * <p>
 * <b>Configuration example:</b>
 * <pre>{@code
 * IncomingEventTransport transport = new KafkaIncomingEventTransport(
 *     "localhost:9092",
 *     "events",
 *     "my-group"
 * );
 * transport.start(event -> dispatcher.dispatch(event));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-06
 */
@Slf4j
public class KafkaIncomingEventTransport implements IncomingEventTransport {

    private final Consumer<String, String> kafkaConsumer;
    private final List<String> topics;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Poll timeout in milliseconds.
     */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    /**
     * Create Kafka transport with bootstrap servers, topics, and group ID.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topicsConfig     comma-separated list of topics to subscribe to
     * @param groupId          consumer group ID
     */
    public KafkaIncomingEventTransport(String bootstrapServers, String topicsConfig, String groupId) {
        this(createConsumer(bootstrapServers, groupId), parseTopics(topicsConfig), Executors.newSingleThreadExecutor());
    }

    /**
     * Create Kafka transport with custom properties, topics, and group ID.
     *
     * @param properties   Kafka consumer properties
     * @param topicsConfig comma-separated list of topics to subscribe to
     * @param groupId      consumer group ID
     */
    public KafkaIncomingEventTransport(Properties properties, String topicsConfig, String groupId) {
        this(createConsumer(properties, groupId), parseTopics(topicsConfig), Executors.newSingleThreadExecutor());
    }

    /**
     * Create Kafka transport with custom consumer (for tests).
     *
     * @param kafkaConsumer Kafka consumer instance
     * @param topics        list of topics to subscribe to
     */
    public KafkaIncomingEventTransport(Consumer<String, String> kafkaConsumer, List<String> topics) {
        this(kafkaConsumer, topics, Executors.newSingleThreadExecutor());
    }

    /**
     * Create Kafka transport with custom consumer and executor (for tests).
     *
     * @param kafkaConsumer   Kafka consumer instance
     * @param topics          list of topics to subscribe to
     * @param executorService executor service for running the consumer loop
     */
    public KafkaIncomingEventTransport(Consumer<String, String> kafkaConsumer,
                                       List<String> topics,
                                       ExecutorService executorService) {
        this.topics = topics;
        this.kafkaConsumer = kafkaConsumer;
        this.executorService = executorService;
    }

    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public void start(java.util.function.Consumer<Event> eventConsumer) {
        if (running.compareAndSet(false, true)) {
            executorService.execute(() -> consumeLoop(eventConsumer));
            log.info("KafkaIncomingEventTransport started, listening to topics: {}", topics);
        } else {
            log.warn("KafkaIncomingEventTransport is already running");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping KafkaIncomingEventTransport...");
            if (kafkaConsumer != null) {
                tryStopConsumer();
            }
            executorService.shutdownNow();
            log.info("KafkaIncomingEventTransport stopped");
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
                log.error("KafkaIncomingEventTransport loop error", e);
            }
        }
    }

    private void tryPollRecords(Consumer<String, String> kafkaConsumer,
                                java.util.function.Consumer<Event> eventConsumer) {
        try {
            for (ConsumerRecord<String, String> record : kafkaConsumer.poll(POLL_TIMEOUT)) {
                tryDeliverEvent(record, eventConsumer);
            }
        } catch (Exception e) {
            if (running.get()) {
                log.error("Error during Kafka message processing", e);
            }
        }
    }

    private void tryDeliverEvent(ConsumerRecord<String, String> record,
                                 java.util.function.Consumer<Event> eventConsumer) {
        try {
            Event event = EventUtils.fromJson(record.value(), Event.class);
            eventConsumer.accept(event);
            log.debug("Event delivered from Kafka topic: {}, key: {}, event: {}",
                    record.topic(), record.key(), event.asJson());
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

    private static Consumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        return new KafkaConsumer<>(props);
    }

    private static Consumer<String, String> createConsumer(Properties properties, String groupId) {
        Properties props = new Properties();
        props.putAll(properties);
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private static List<String> parseTopics(String topicsConfig) {
        return List.of(topicsConfig.split(","));
    }
}
