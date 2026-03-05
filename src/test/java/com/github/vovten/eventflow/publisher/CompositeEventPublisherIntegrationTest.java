package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventBus;
import com.github.vovten.eventflow.TestEventListener;
import com.github.vovten.eventflow.dispatcher.ExternalEventDispatcher;
import com.github.vovten.eventflow.test.CompositeTestEvent;
import com.github.vovten.eventflow.TestEvent;
import com.github.vovten.eventflow.registry.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringAnnotationEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceEventListenerRegistry;
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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for CompositeEventPublisher
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
        topics = {"test-events"}
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CompositeEventPublisherIntegrationTest {

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaBrokers;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private TestEventListener eventListener;

    @Autowired
    private BlockingDeque<Event> eventQueue;

    private EventPublisher compositeEventPublisher;
    private ExternalEventDispatcher externalDispatcher;
    private ExecutorService dispatcherExecutor;

    @BeforeEach
    void setUp() {
        // Очищаем очередь внутренних событий
        eventQueue.clear();

        // Настраиваем слушатель для внешних событий
        eventListener.setAnnotationResult(null);
        eventListener.setInterfaceResult(null);
        eventListener.setCompositeResult(null);

        // Создаем внутренний publisher
        InternalEventPublisher internalPublisher = new InternalEventPublisher(eventQueue);

        // Создаем внешний publisher с реальным KafkaTemplate
        ExternalEventPublisher externalPublisher = new ExternalEventPublisher(
                createKafkaTemplate(),
                "test-events"
        );

        // Создаем композитный publisher с обоими publisher'ами
        Map<EventBus, EventPublisher> publishers = new HashMap<>();
        publishers.put(EventBus.INTERNAL, internalPublisher);
        publishers.put(EventBus.EXTERNAL, externalPublisher);
        compositeEventPublisher = new CompositeEventPublisher(publishers, true);

        // Настраиваем и запускаем dispatcher для получения внешних событий
        dispatcherExecutor = Executors.newSingleThreadExecutor();
        
        // Создаем реестры явно для тестов
        var annotationRegistry = new SpringAnnotationEventListenerRegistry(
                TestEvent.class.getPackageName(), applicationContext);
        var interfaceRegistry = new SpringInterfaceEventListenerRegistry(applicationContext);
        var listenerRegistry = new CompositeEventListenerRegistry(
                java.util.List.of(annotationRegistry, interfaceRegistry));
        
        externalDispatcher = new ExternalEventDispatcher(
                createDispatcherConsumer(),
                java.util.List.of("test-events"),
                dispatcherExecutor,
                listenerRegistry
        );
        externalDispatcher.start();
    }

    @AfterEach
    void tearDown() {
        if (externalDispatcher != null) {
            externalDispatcher.stop();
        }
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdown();
        }
        eventQueue.clear();
    }

    @Test
    @DisplayName("Should publish event to INTERNAL bus via composite publisher")
    void shouldPublishEventToInternalBusViaCompositePublisher() throws InterruptedException {
        // given
        TestEvent event = new TestEvent("Composite test message");

        // when
        compositeEventPublisher.publish(event);

        // then
        Event publishedEvent = eventQueue.poll(5, TimeUnit.SECONDS);
        assertNotNull(publishedEvent);
        assertEquals("Composite test message", ((TestEvent) publishedEvent).getMessage());
    }

    @Test
    @DisplayName("Should publish event to EXTERNAL bus via composite publisher")
    void shouldPublishEventToExternalBusViaCompositePublisher() throws InterruptedException {
        // given
        TestEvent event = new TestEvent("test-msg-1");
        CountDownLatch latch = new CountDownLatch(1);
        eventListener.setLatch(latch);

        // when - публикуем событие через композитный publisher
        // Событие должно быть отправлено на внешнюю шину (Kafka)
        compositeEventPublisher.publish(event);

        // Ждем получения события через dispatcher
        boolean completed = latch.await(5, SECONDS);

        // then
        assertTrue(completed, "Event should be processed within timeout");
        assertEquals("test-msg-1", eventListener.getAnnotationResult());
        assertEquals("test-msg-1", eventListener.getInterfaceResult());
    }

    @Test
    @DisplayName("Should handle event for both INTERNAL and EXTERNAL buses")
    void shouldHandleEventForBothBuses() throws InterruptedException {
        // given
        CompositeTestEvent event = CompositeTestEvent.create("Both buses test");

        // Для внешнего события
        CountDownLatch latch = new CountDownLatch(1);
        eventListener.setLatch(latch);

        // when - публикуем событие, которое должно быть отправлено на обе шины
        compositeEventPublisher.publish(event);

        // then - проверяем получение внутреннего события
        Event internalEvent = eventQueue.poll(5, TimeUnit.SECONDS);
        assertNotNull(internalEvent);
        assertEquals("Both buses test", ((CompositeTestEvent) internalEvent).getMessage());

        // then - проверяем получение внешнего события через Kafka
        boolean externalCompleted = latch.await(5, SECONDS);
        assertTrue(externalCompleted, "External event should be processed within timeout");
        assertEquals("Both buses test", eventListener.getCompositeResult());
    }

    @Test
    @DisplayName("Should throw exception when trying to get event bus")
    void shouldThrowExceptionWhenTryingToGetEventBus() {
        // when & then
        assertThrows(UnsupportedOperationException.class,
                () -> compositeEventPublisher.eventBus(),
                "Composite publisher should not support direct eventBus() method"
        );
    }

    private KafkaTemplate<String, String> createKafkaTemplate() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<>(properties);
        return new KafkaTemplate<>(producerFactory);
    }

    private Consumer<String, String> createDispatcherConsumer() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-composite-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        DefaultKafkaConsumerFactory<String, String> factory =
                new DefaultKafkaConsumerFactory<>(properties);
        return factory.createConsumer();
    }
}