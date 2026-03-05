package com.github.vovten.eventflow;

import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.dispatcher.ExternalEventDispatcher;
import com.github.vovten.eventflow.publisher.ChannelEventPublisher;
import com.github.vovten.eventflow.publisher.EventPublisher;
import com.github.vovten.eventflow.registry.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringAnnotationEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceEventListenerRegistry;
import com.github.vovten.eventflow.transport.KafkaEventTransport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" },
        topics = { "test-events" }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExternalPublisherDispatcherIntegrationTest {

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaBrokers;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    TestEventListener eventListener;

    private EventPublisher publisher;
    private ExternalEventDispatcher dispatcher;
    private ExecutorService dispatcherExecutor;
    private String uniqueGroupId;

    @BeforeEach
    void setUp() throws InterruptedException {
        uniqueGroupId = "test-dispatcher-group-" + UUID.randomUUID();

        // Создаём каналы и publisher для тестов
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        var kafkaTransport = new KafkaEventTransport(kafkaProps, "test-events");
        var externalChannel = new ExternalEventChannel(List.of(kafkaTransport));
        
        publisher = new ChannelEventPublisher(List.of(externalChannel), false);

        // Создаем реестры явно для тестов
        var annotationRegistry = new SpringAnnotationEventListenerRegistry(
                TestEvent.class.getPackageName(), applicationContext);
        var interfaceRegistry = new SpringInterfaceEventListenerRegistry(applicationContext);
        var listenerRegistry = new CompositeEventListenerRegistry(
                java.util.List.of(annotationRegistry, interfaceRegistry));

        dispatcherExecutor = Executors.newSingleThreadExecutor();
        dispatcher = new ExternalEventDispatcher(
                createDispatcherConsumer(),
                List.of("test-events"),
                dispatcherExecutor,
                listenerRegistry
        );

        dispatcher.start();

        // Ждем инициализации
        Thread.sleep(1000);
    }

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.stop();
        }
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdown();
            try {
                if (!dispatcherExecutor.awaitTermination(2, SECONDS)) {
                    dispatcherExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                dispatcherExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @DisplayName("Should publish event to Kafka topic")
    void shouldPublishEventToKafkaTopic() {
        // arrange
        TestEvent testEvent = new TestEvent("test-id-123");

        // act
        publisher.publish(testEvent);

        // assert
        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertEquals("test-id-123", eventListener.getAnnotationResult());
            assertEquals("test-id-123", eventListener.getInterfaceResult());
        });
    }

    private Consumer<String, String> createDispatcherConsumer() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(properties);
        return factory.createConsumer();
    }
}