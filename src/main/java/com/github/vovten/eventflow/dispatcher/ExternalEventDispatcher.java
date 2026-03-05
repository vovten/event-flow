package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.registry.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringAnnotationEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceEventListenerRegistry;
import com.github.vovten.eventflow.util.EventUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Event dispatcher that listens to events from the external bus (external channel).
 *
 * @author Vladimir Aleshkov
 * @since 21.11.2024
 */
@Slf4j
public class ExternalEventDispatcher extends AbstractEventDispatcher {

    private Future<?> consumerFuture;
    private final List<String> topics;
    private final ExecutorService executorService;
    private final Consumer<String, String> kafkaConsumer;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    /**
     * Constructor for tests - allows injecting a mock Consumer
     */
    public ExternalEventDispatcher(Consumer<String, String> kafkaConsumer,
                                   List<String> topics,
                                   ExecutorService executorService) {
        this(kafkaConsumer, topics, executorService, "");
    }

    /**
     * Constructor for tests - allows injecting a mock Consumer with event listener scan package
     */
    public ExternalEventDispatcher(Consumer<String, String> kafkaConsumer,
                                   List<String> topics,
                                   ExecutorService executorService,
                                   String eventListenerScanPackage) {
        super(executorService, eventListenerScanPackage);
        this.topics = topics;
        this.kafkaConsumer = kafkaConsumer;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * Constructor for tests - allows injecting a mock Consumer with custom listener registry
     */
    public ExternalEventDispatcher(Consumer<String, String> kafkaConsumer,
                                   List<String> topics,
                                   ExecutorService executorService,
                                   EventListenerRegistry listenerRegistry) {
        super(executorService, "", listenerRegistry);
        this.topics = topics;
        this.kafkaConsumer = kafkaConsumer;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * Spring constructor - creates Kafka Consumer from configuration
     */
    public ExternalEventDispatcher(String bootstrapServers,
                                   String topicsConfig,
                                   String groupId,
                                   String eventListenerScanPackage,
                                   ExecutorService executorService) {
        super(executorService, eventListenerScanPackage);
        this.kafkaConsumer = createConsumer(bootstrapServers, groupId);
        this.topics = parseTopics(topicsConfig);
        this.executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * Spring constructor with custom listener registry
     */
    public ExternalEventDispatcher(String bootstrapServers,
                                   String topicsConfig,
                                   String groupId,
                                   String eventListenerScanPackage,
                                   ExecutorService executorService,
                                   EventListenerRegistry listenerRegistry) {
        super(executorService, eventListenerScanPackage, listenerRegistry);
        this.kafkaConsumer = createConsumer(bootstrapServers, groupId);
        this.topics = parseTopics(topicsConfig);
        this.executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * Starts the Kafka consumer in a background thread
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            consumerFuture = executorService.submit(this::consumeLoop);
            log.info("ExternalEventDispatcher started, listening to topics: {}", topics);
        }
    }

    /**
     * Stops the Kafka consumer
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping ExternalEventDispatcher...");
            if (consumerFuture != null) {
                consumerFuture.cancel(true);
            }
            if (kafkaConsumer != null) {
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
            log.info("ExternalEventDispatcher stopped");
        }
    }

    private void consumeLoop() {
        try {
            kafkaConsumer.subscribe(topics);
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, String> record : records) {
                        processRecord(record);
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        log.error("Error during Kafka message processing", e);
                    }
                }
            }
        } catch (Exception e) {
            if (running.get()) {
                log.error("Kafka consumer loop error", e);
            }
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            Event event = EventUtils.fromJson(record.value(), Event.class);
            dispatch(event);
            log.debug("Event received from topic: {}, key: {}, event: {}", record.topic(), record.key(), event.asJson());
        } catch (Exception e) {
            log.error("Failed to process event from topic: {}, key: {}", record.topic(), record.key(), e);
        }
    }

    private Consumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    private List<String> parseTopics(String topicsConfig) {
        return List.of(topicsConfig.split(","));
    }
}
