package io.github.vovten.eventflow;

import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import io.github.vovten.eventflow.publisher.ChannelEventPublisher;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.registry.CompositeEventHandlerRegistry;
import io.github.vovten.eventflow.registry.SpringEventListenerRegistry;
import io.github.vovten.eventflow.registry.SpringEventSubscriberRegistry;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.ExternalTestEvent;
import io.github.vovten.eventflow.transport.incoming.KafkaInTransport;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import io.github.vovten.eventflow.transport.outgoing.KafkaOutTransport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = "event-flow.enabled=false")
@ImportAutoConfiguration(exclude = io.github.vovten.eventflow.autoconfig.EventFlowDisabledAutoConfiguration.class)
@EmbeddedKafka(
        partitions = 1,
        topics = {"test-events"}
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExternalPublisherDispatcherIntegrationTest {

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaBrokers;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    TestEventListener eventListener;

    private EventPublisher publisher;
    private UnifiedEventDispatcher dispatcher;
    private ExecutorService dispatcherExecutor;
    private String uniqueGroupId;

    @BeforeEach
    void setUp() throws InterruptedException {
        uniqueGroupId = "test-dispatcher-group-" + UUID.randomUUID();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        var kafkaTransport = new KafkaOutTransport(kafkaProps, "test-events");
        var externalChannel = new ExternalEventChannel(List.of(kafkaTransport));
        var transport = new LocalQueueOutTransport(new LinkedBlockingDeque<>(1000));
        var internalChannel = new InternalEventChannel(List.of(transport));

        publisher = new ChannelEventPublisher(List.of(internalChannel, externalChannel));
        dispatcherExecutor = Executors.newFixedThreadPool(2);
        KafkaInTransport kafkaInTransport = new KafkaInTransport(
                createDispatcherConsumer(),
                List.of("test-events"),
                dispatcherExecutor,
                new EventSerializerFactory()
        );
        dispatcher = new UnifiedEventDispatcher(
                dispatcherExecutor,
                createEventHandlerRegistry(),
                List.of(kafkaInTransport)
        );
        dispatcher.start(dispatcher::dispatch);

        // Wait for consumer to subscribe
        Thread.sleep(3000);
    }

    private CompositeEventHandlerRegistry createEventHandlerRegistry() {
        var scanPackage = TestEvent.class.getPackageName();
        var annotationRegistry = new SpringEventListenerRegistry(applicationContext, scanPackage);
        annotationRegistry.postConstructInitialize();
        var subscriberRegistry = new SpringEventSubscriberRegistry(applicationContext);
        subscriberRegistry.postConstructInitialize();
        return new CompositeEventHandlerRegistry(List.of(annotationRegistry, subscriberRegistry));
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
    void shouldPublishEventToKafkaTopic() throws InterruptedException {
        // arrange
        ExternalTestEvent testEvent = new ExternalTestEvent("test-id-123", "test-payload");
        eventListener.setLatch(new CountDownLatch(1));

        // act
        publisher.publish(testEvent).join();

        // assert - wait for event to be received
        boolean completed = eventListener.getLatch().await(25, SECONDS);
        assertTrue(completed, "Event should be received within timeout");
        assertEquals("test-id-123", eventListener.getAnnotationResult());
        assertEquals("test-id-123", eventListener.getInterfaceResult());
    }

    private KafkaConsumer<String, byte[]> createDispatcherConsumer() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        DefaultKafkaConsumerFactory<String, byte[]> factory = new DefaultKafkaConsumerFactory<>(properties);
        return (KafkaConsumer<String, byte[]>) factory.createConsumer();
    }
}
