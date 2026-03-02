package com.github.vovten.eventflow.event.dispatcher;

import com.github.vovten.eventflow.event.EventFlowTestApplication;
import com.github.vovten.eventflow.event.test.TestEvent;
import com.github.vovten.eventflow.event.annotation.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for InternalEventDispatcher
 */
@SpringBootTest(classes = EventFlowTestApplication.class,
    properties = {
        "event.internal.enabled=true",
        "event.internal.dispatcher.enabled=true",
        "event.listener.scan.package=com.github.vovten.eventflow"
    })
@ActiveProfiles("test")
class InternalEventDispatcherIntegrationTest {

    @Autowired
    private BlockingDeque<com.github.vovten.eventflow.event.Event> eventQueue;

    @Autowired
    private ApplicationContext applicationContext;

    private TestEventListener testEventListener;

    @BeforeEach
    void setUp() {
        eventQueue.clear();
        testEventListener = new TestEventListener();
        applicationContext.getAutowireCapableBeanFactory().autowireBean(testEventListener);
    }

    @Test
    @DisplayName("Should dispatch event from queue to listener")
    void shouldDispatchEventFromQueueToListener() throws InterruptedException {
        // given
        TestEvent event = TestEvent.create("Dispatcher test message");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestEvent> receivedEvent = new AtomicReference<>();
        
        testEventListener.setLatch(latch);
        testEventListener.setEventConsumer(receivedEvent::set);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(testEventListener);

        // when
        eventQueue.put(event);
        boolean awaited = latch.await(5, TimeUnit.SECONDS);

        // then
        assertTrue(awaited, "Event was not processed in time");
        assertNotNull(receivedEvent.get());
        assertEquals("Dispatcher test message", receivedEvent.get().getMessage());
    }

    @Test
    @DisplayName("Should handle multiple events from queue")
    void shouldHandleMultipleEventsFromQueue() throws InterruptedException {
        // given
        CountDownLatch latch = new CountDownLatch(3);
        AtomicReference<Integer> eventCount = new AtomicReference<>(0);
        
        testEventListener.setLatch(latch);
        testEventListener.setEventConsumer(e -> eventCount.updateAndGet(v -> v + 1));
        applicationContext.getAutowireCapableBeanFactory().autowireBean(testEventListener);

        // when
        eventQueue.put(TestEvent.create("First"));
        eventQueue.put(TestEvent.create("Second"));
        eventQueue.put(TestEvent.create("Third"));
        
        boolean awaited = latch.await(5, TimeUnit.SECONDS);

        // then
        assertTrue(awaited, "Not all events were processed in time");
        assertEquals(3, eventCount.get().intValue());
    }

    // Test listener class
    @org.springframework.stereotype.Component
    static class TestEventListener {
        private CountDownLatch latch;
        private java.util.function.Consumer<TestEvent> eventConsumer;

        @EventListener
        public void handleTestEvent(TestEvent event) {
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

        void setEventConsumer(java.util.function.Consumer<TestEvent> consumer) {
            this.eventConsumer = consumer;
        }
    }
}
