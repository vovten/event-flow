package com.github.vovten.eventflow.event.publisher;

import com.github.vovten.eventflow.event.EventBus;
import com.github.vovten.eventflow.event.EventFlowTestApplication;
import com.github.vovten.eventflow.event.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for InternalEventPublisher
 */
@SpringBootTest(classes = EventFlowTestApplication.class)
@ActiveProfiles("test")
class InternalEventPublisherIntegrationTest {

    @Autowired
    private InternalEventPublisher publisher;

    @Autowired
    private BlockingDeque<com.github.vovten.eventflow.event.Event> eventQueue;

    @BeforeEach
    void setUp() {
        eventQueue.clear();
    }

    @Test
    @DisplayName("Should publish event to internal queue via Spring context")
    void shouldPublishEventToInternalQueueViaSpringContext() throws InterruptedException {
        // given
        TestEvent event = TestEvent.create("Integration test message");

        // when
        publisher.publish(event);

        // then
        com.github.vovten.eventflow.event.Event publishedEvent = eventQueue.poll(5, TimeUnit.SECONDS);
        assertNotNull(publishedEvent);
        assertTrue(publishedEvent instanceof TestEvent);
        assertEquals("Integration test message", ((TestEvent) publishedEvent).getMessage());
    }

    @Test
    @DisplayName("Should return INTERNAL event bus from Spring bean")
    void shouldReturnInternalEventBusFromSpringBean() {
        // when
        EventBus eventBus = publisher.eventBus();

        // then
        assertEquals(EventBus.INTERNAL, eventBus);
    }

    @Test
    @DisplayName("Should handle multiple events sequentially")
    void shouldHandleMultipleEventsSequentially() throws InterruptedException {
        // given
        TestEvent event1 = TestEvent.create("First");
        TestEvent event2 = TestEvent.create("Second");
        TestEvent event3 = TestEvent.create("Third");

        // when
        publisher.publish(event1);
        publisher.publish(event2);
        publisher.publish(event3);

        // then
        com.github.vovten.eventflow.event.Event e1 = eventQueue.poll(5, TimeUnit.SECONDS);
        com.github.vovten.eventflow.event.Event e2 = eventQueue.poll(5, TimeUnit.SECONDS);
        com.github.vovten.eventflow.event.Event e3 = eventQueue.poll(5, TimeUnit.SECONDS);

        assertNotNull(e1);
        assertNotNull(e2);
        assertNotNull(e3);
        assertEquals("First", ((TestEvent) e1).getMessage());
        assertEquals("Second", ((TestEvent) e2).getMessage());
        assertEquals("Third", ((TestEvent) e3).getMessage());
    }
}
