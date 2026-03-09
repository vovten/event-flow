package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.transport.OutgoingEventTransportException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.LocalDateTime;
import java.util.concurrent.BlockingDeque;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link InMemoryOutgoingEventTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("InMemoryOutgoingEventTransport Tests")
class InMemoryOutgoingEventTransportTest {

    @Test
    @DisplayName("Should create with default queue size")
    void shouldCreateWithDefaultQueueSize() {
        // Act
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport();

        // Assert
        assertNotNull(transport);
        assertEquals("in-memory", transport.name());
    }

    @Test
    @DisplayName("Should create with custom queue size")
    void shouldCreateWithCustomQueueSize() {
        // Act
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(100);

        // Assert
        assertNotNull(transport);
        assertEquals(100, transport.getEventQueue().remainingCapacity() + transport.getEventQueue().size());
    }

    @Test
    @DisplayName("Should send event to queue")
    void shouldSendEventToQueue() {
        // Arrange
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport();
        TestEvent event = new TestEvent("test");

        // Act
        transport.send(event);

        // Assert
        assertEquals(1, transport.getEventQueue().size());
        assertEquals(event, transport.getEventQueue().peek());
    }

    @Test
    @DisplayName("Should throw exception when queue is full")
    void shouldThrowExceptionWhenQueueIsFull() {
        // Arrange
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(2);
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

    @Test
    @DisplayName("Should return event queue")
    void shouldReturnEventQueue() {
        // Arrange
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport();

        // Act
        BlockingDeque<Event> queue = transport.getEventQueue();

        // Assert
        assertNotNull(queue);
    }

    /**
     * Test event class.
     */
    private static class TestEvent implements Event {
        private final String data;

        public TestEvent(String data) {
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
