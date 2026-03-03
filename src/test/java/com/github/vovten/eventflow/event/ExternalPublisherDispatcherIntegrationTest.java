package com.github.vovten.eventflow.event;

import com.github.vovten.eventflow.event.dispatcher.ExternalEventDispatcher;
import com.github.vovten.eventflow.event.publisher.ExternalEventPublisher;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for ExternalEventPublisher with Embedded Kafka
 */
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" },
    topics = { "test-events" }
)
class ExternalPublisherDispatcherIntegrationTest {

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaBrokers;
    @Autowired
    ApplicationContext applicationContext;
    @Autowired
    TestEventListener eventListener;

    private ExternalEventPublisher publisher;
    private ExternalEventDispatcher dispatcher;
    private ExecutorService dispatcherExecutor;

    @BeforeEach
    void setUp() throws InterruptedException {
        eventListener.setLatch(new CountDownLatch(1));
        eventListener.setAnnotationResult(null);
        
        publisher = new ExternalEventPublisher(
                createKafkaTemplate(),
                "test-events"
        );
        dispatcherExecutor = Executors.newSingleThreadExecutor();
        dispatcher = new ExternalEventDispatcher(
                createDispatcherConsumer(),
                List.of("test-events"),
                dispatcherExecutor,
                "com.github.vovten.eventflow.event"
        );
        dispatcher.start();
        dispatcher.onApplicationEvent(new ContextRefreshedEvent(applicationContext));
    }

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.stop();
            dispatcher.stop();
        }
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdown();
        }
    }

    @Test
    @DisplayName("Should publish event to Kafka topic")
    void shouldPublishEventToKafkaTopic() throws InterruptedException {
        // arrange
        TestEvent testEvent = new TestEvent("test-id-123");
        CountDownLatch latch = new CountDownLatch(1);
        eventListener.setLatch(latch);

        // act
        publisher.publish(testEvent);
        boolean completed = latch.await(5, SECONDS);

        // assert
        assertTrue(completed, "Event should be processed within timeout");
        assertEquals("test-id-123", eventListener.getAnnotationResult());
        assertEquals("test-id-123", eventListener.getInterfaceResult());
    }

    private KafkaTemplate<String, String> createKafkaTemplate() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(properties);
        return new KafkaTemplate<>(producerFactory);
    }

    private Consumer<String, String> createDispatcherConsumer() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dispatcher-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(properties);
        return factory.createConsumer();
    }
}
