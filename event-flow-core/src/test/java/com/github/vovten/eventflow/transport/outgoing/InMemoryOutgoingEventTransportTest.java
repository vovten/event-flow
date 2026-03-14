package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.OutgoingEventTransportException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link InMemoryOutgoingEventTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("InMemoryOutgoingEventTransport Tests")
class InMemoryOutgoingEventTransportTest {

    @Test
    @DisplayName("Should create transport with queue")
    void shouldCreateTransportWithQueue() {
        // Arrange
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(100);

        // Act
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(queue);

        // Assert
        assertNotNull(transport);
        assertEquals("in-memory", transport.name());
    }

    @Test
    @DisplayName("Should send event to queue")
    void shouldSendEventToQueue() throws InterruptedException {
        // Arrange
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(queue);
        TestEvent event = new TestEvent("test");

        // Act
        transport.send(event);

        // Assert
        assertEquals(1, queue.size());
        assertEquals(event, queue.peek());
    }

    @Test
    @DisplayName("Should throw exception when queue is full")
    void shouldThrowExceptionWhenQueueIsFull() {
        // Arrange
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(2);
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(queue);
        TestEvent event1 = new TestEvent("test1");
        TestEvent event2 = new TestEvent("test2");
        TestEvent event3 = new TestEvent("test3");

        // Act
        transport.send(event1);
        transport.send(event2);

        // Assert
        OutgoingEventTransportException exception = assertThrows(OutgoingEventTransportException.class, () ->
                transport.send(event3)
        );
        assertTrue(exception.getMessage().contains("Queue is full"));
    }

    /**
     * Test event class.
     */
    private static class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\",\"timestamp\":\"" + LocalDateTime.now() + "\"}";
        }
    }
}
