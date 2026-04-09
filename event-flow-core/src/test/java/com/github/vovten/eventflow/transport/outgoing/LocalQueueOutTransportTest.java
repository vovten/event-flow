package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link LocalQueueOutTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("LocalQueuePublisherTransport Tests")
class LocalQueueOutTransportTest {

    @Test
    @DisplayName("Should create transport with queue")
    void shouldCreateTransportWithQueue() {
        // Arrange
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(100);

        // Act
        LocalQueueOutTransport transport = new LocalQueueOutTransport(queue);

        // Assert
        assertNotNull(transport);
        assertEquals("local-queue", transport.name());
    }

    @Test
    @DisplayName("Should send event to queue")
    void shouldSendEventToQueue() throws InterruptedException {
        // Arrange
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueOutTransport transport = new LocalQueueOutTransport(queue);
        TestEvent event = new TestEvent("test");

        // Act
        transport.send(event);

        // Assert
        assertEquals(1, queue.size());
        assertEquals(event, queue.peek());
    }

    @Test
    @DisplayName("Should return failed result when queue is full")
    void shouldReturnFailedResultWhenQueueIsFull() {
        // Arrange
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(2);
        LocalQueueOutTransport transport = new LocalQueueOutTransport(queue);
        TestEvent event1 = new TestEvent("test1");
        TestEvent event2 = new TestEvent("test2");
        TestEvent event3 = new TestEvent("test3");

        // Act
        transport.send(event1);
        transport.send(event2);
        CompletableFuture<SendResult> future = transport.send(event3);

        // Assert
        SendResult result = future.join();
        assertFalse(result.success());
        assertTrue(result.errorDetails().contains("Queue is full"));
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
