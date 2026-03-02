package com.github.vovten.eventflow.event.dispatcher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.event.EventFlowTestApplication;
import com.github.vovten.eventflow.event.test.ExternalTestEvent;
import com.github.vovten.eventflow.event.annotation.EventListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ExternalEventDispatcher with Embedded Kafka
 */
@SpringBootTest(classes = EventFlowTestApplication.class)
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" },
    topics = { "test-external-events" }
)
@TestPropertySource(properties = {
    "event.external.dispatcher.enabled=true",
    "event.external.dispatcher.topics=test-external-events",
    "kafka.bootstrap.servers=localhost:9092",
    "event.listener.scan.package=com.github.vovten.eventflow"
})
@ActiveProfiles("test")
@DirtiesContext
class ExternalEventDispatcherIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ApplicationContext applicationContext;

    private TestEventListener testEventListener;

    @BeforeEach
    void setUp() {
        testEventListener = new TestEventListener();
        applicationContext.getAutowireCapableBeanFactory().autowireBean(testEventListener);
    }

    @Test
    @DisplayName("Should consume event from Kafka and dispatch to listener")
    void shouldConsumeEventFromKafkaAndDispatchToListener() throws InterruptedException {
        // given
        ExternalTestEvent event = ExternalTestEvent.create("Kafka dispatcher test");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ExternalTestEvent> receivedEvent = new AtomicReference<>();
        
        testEventListener.setLatch(latch);
        testEventListener.setEventConsumer(receivedEvent::set);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(testEventListener);

        // when
        kafkaTemplate.send(new ProducerRecord<>("test-external-events", event.asJson()));
        boolean awaited = latch.await(10, TimeUnit.SECONDS);

        // then
        assertTrue(awaited, "Event was not processed in time");
        assertNotNull(receivedEvent.get());
        assertEquals("Kafka dispatcher test", receivedEvent.get().getMessage());
    }

    @Test
    @DisplayName("Should handle multiple events from Kafka")
    void shouldHandleMultipleEventsFromKafka() throws InterruptedException {
        // given
        CountDownLatch latch = new CountDownLatch(3);
        AtomicReference<Integer> eventCount = new AtomicReference<>(0);
        
        testEventListener.setLatch(latch);
        testEventListener.setEventConsumer(e -> eventCount.updateAndGet(v -> v + 1));
        applicationContext.getAutowireCapableBeanFactory().autowireBean(testEventListener);

        // when
        kafkaTemplate.send(new ProducerRecord<>("test-external-events", 
            ExternalTestEvent.create("First").asJson()));
        kafkaTemplate.send(new ProducerRecord<>("test-external-events", 
            ExternalTestEvent.create("Second").asJson()));
        kafkaTemplate.send(new ProducerRecord<>("test-external-events", 
            ExternalTestEvent.create("Third").asJson()));
        
        boolean awaited = latch.await(10, TimeUnit.SECONDS);

        // then
        assertTrue(awaited, "Not all events were processed in time");
        assertEquals(3, eventCount.get().intValue());
    }

    // Test listener class
    @org.springframework.stereotype.Component
    static class TestEventListener {
        private CountDownLatch latch;
        private java.util.function.Consumer<ExternalTestEvent> eventConsumer;

        @EventListener
        public void handleExternalTestEvent(ExternalTestEvent event) {
            if (eventConsumer != null) {
                eventConsumer.accept(event);
            }
            if (latch != null) {
                latch.countDown();
            }
        }

        void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        void setEventConsumer(java.util.function.Consumer<ExternalTestEvent> consumer) {
            this.eventConsumer = consumer;
        }
    }
}
