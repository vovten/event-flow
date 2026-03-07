package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import com.github.vovten.eventflow.transport.OutgoingEventTransportException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InMemoryOutgoingEventTransport.
 */
@DisplayName("InMemoryOutgoingEventTransport Tests")
class InMemoryOutgoingEventTransportTest {

    @Test
    @DisplayName("Should create transport with default constructor")
    void shouldCreateTransportWithDefaultConstructor() {
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport();

        assertEquals("in-memory", transport.name());
        assertNotNull(transport.getEventQueue());
        assertEquals(5000, transport.getEventQueue().remainingCapacity() + transport.getEventQueue().size());
    }

    @Test
    @DisplayName("Should create transport with custom queue size")
    void shouldCreateTransportWithCustomQueueSize() {
        int maxQueueSize = 100;
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(maxQueueSize);

        assertEquals("in-memory", transport.name());
        assertEquals(maxQueueSize, transport.getEventQueue().remainingCapacity() + transport.getEventQueue().size());
    }

    @Test
    @DisplayName("Should add event to queue on send")
    void shouldAddEventToQueueOnSend() throws InterruptedException {
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(10);
        TestEvent event = new TestEvent();

        transport.send(event);

        assertEquals(1, transport.getEventQueue().size());
        assertEquals(event, transport.getEventQueue().take());
    }

    @Test
    @DisplayName("Should throw exception when queue is full")
    void shouldThrowExceptionWhenQueueIsFull() {
        int maxQueueSize = 2;
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(maxQueueSize);
        TestEvent event1 = new TestEvent();
        TestEvent event2 = new TestEvent();
        TestEvent event3 = new TestEvent();

        transport.send(event1);
        transport.send(event2);

        OutgoingEventTransportException exception = assertThrows(
                OutgoingEventTransportException.class,
                () -> transport.send(event3)
        );
        assertTrue(exception.getMessage().contains("Queue is full"));
    }

    @Test
    @DisplayName("Should return internal queue")
    void shouldReturnInternalQueue() {
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(10);
        BlockingDeque<Event> queue = transport.getEventQueue();

        assertNotNull(queue);
        TestEvent event = new TestEvent();
        transport.send(event);

        assertEquals(1, queue.size());
    }

    @Test
    @DisplayName("Should preserve order on multiple sends")
    void shouldPreserveOrderOnMultipleSends() throws InterruptedException {
        InMemoryOutgoingEventTransport transport = new InMemoryOutgoingEventTransport(10);
        TestEvent event1 = new TestEvent("event1");
        TestEvent event2 = new TestEvent("event2");
        TestEvent event3 = new TestEvent("event3");

        transport.send(event1);
        transport.send(event2);
        transport.send(event3);

        BlockingDeque<Event> queue = transport.getEventQueue();
        assertEquals(event1, queue.take());
        assertEquals(event2, queue.take());
        assertEquals(event3, queue.take());
    }

    static class TestEvent implements Event {
        private final String id;

        TestEvent(String id) {
            this.id = id;
        }

        TestEvent() {
            this("default");
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof TestEvent)) return false;
            TestEvent other = (TestEvent) obj;
            return id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }
}
