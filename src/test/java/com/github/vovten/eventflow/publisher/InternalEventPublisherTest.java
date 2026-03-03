package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventBus;
import com.github.vovten.eventflow.test.ExternalTestEvent;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InternalEventPublisher
 */
@ExtendWith(MockitoExtension.class)
class InternalEventPublisherTest {

    private BlockingDeque<Event> eventQueue;
    private InternalEventPublisher publisher;

    @BeforeEach
    void setUp() {
        eventQueue = new LinkedBlockingDeque<>();
        publisher = new InternalEventPublisher(eventQueue);
    }

    @Test
    @DisplayName("Should publish event to internal queue")
    void shouldPublishEventToInternalQueue() throws InterruptedException {
        // given
        TestEvent event = TestEvent.create("Test message");

        // when
        publisher.publish(event);

        // then
        assertEquals(1, eventQueue.size());
        Event publishedEvent = eventQueue.take();
        assertEquals("Test message", ((TestEvent) publishedEvent).getMessage());
    }

    @Test
    @DisplayName("Should return INTERNAL event bus")
    void shouldReturnInternalEventBus() {
        // when
        EventBus eventBus = publisher.eventBus();

        // then
        assertEquals(EventBus.INTERNAL, eventBus);
    }

    @Test
    @DisplayName("Should throw exception when event does not support INTERNAL bus")
    void shouldThrowExceptionWhenEventDoesNotSupportInternalBus() {
        // given
        TestEvent event = TestEvent.create();
        // TestEvent only supports INTERNAL, so we need a different event type
        // For this test, we'll use ExternalTestEvent which only supports EXTERNAL
        ExternalTestEvent externalEvent =
            ExternalTestEvent.create();

        // when & then
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> publisher.publish(externalEvent)
        );
        assertTrue(exception.getMessage().contains("INTERNAL"));
    }

    @Test
    @DisplayName("Should handle multiple events in order")
    void shouldHandleMultipleEventsInOrder() throws InterruptedException {
        // given
        TestEvent event1 = TestEvent.create("First");
        TestEvent event2 = TestEvent.create("Second");
        TestEvent event3 = TestEvent.create("Third");

        // when
        publisher.publish(event1);
        publisher.publish(event2);
        publisher.publish(event3);

        // then
        assertEquals(3, eventQueue.size());
        assertEquals("First", ((TestEvent) eventQueue.take()).getMessage());
        assertEquals("Second", ((TestEvent) eventQueue.take()).getMessage());
        assertEquals("Third", ((TestEvent) eventQueue.take()).getMessage());
    }
}
