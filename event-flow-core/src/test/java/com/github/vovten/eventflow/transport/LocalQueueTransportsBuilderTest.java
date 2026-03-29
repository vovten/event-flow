package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link LocalQueueTransportsBuilder}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("InMemoryTransportsBuilder Tests")
class LocalQueueTransportsBuilderTest {

    private ExecutorService testExecutor;

    @BeforeEach
    void setUp() {
        testExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        if (testExecutor != null && !testExecutor.isShutdown()) {
            testExecutor.shutdownNow();
        }
    }

    @Test
    @DisplayName("Should build transports with default queue size")
    void shouldBuildTransportsWithDefaultQueueSize() {
        // Act
        LocalQueueTransportsBuilder.InMemoryTransports transports = new LocalQueueTransportsBuilder()
                .build();

        // Assert
        assertNotNull(transports.dispatcher());
        assertNotNull(transports.publisher());
        assertEquals("in-memory", transports.dispatcher().name());
        assertEquals("in-memory", transports.publisher().name());
    }

    @Test
    @DisplayName("Should build transports with custom queue size")
    void shouldBuildTransportsWithCustomQueueSize() {
        // Act
        LocalQueueTransportsBuilder.InMemoryTransports transports = new LocalQueueTransportsBuilder()
                .queueSize(50)
                .build();

        // Assert
        assertNotNull(transports.dispatcher());
        assertNotNull(transports.publisher());
    }

    @Test
    @DisplayName("Should build transports with custom queue")
    void shouldBuildTransportsWithCustomQueue() {
        // Arrange
        BlockingDeque<Event> customQueue = new LinkedBlockingDeque<>(100);

        // Act
        LocalQueueTransportsBuilder.InMemoryTransports transports = new LocalQueueTransportsBuilder()
                .queue(customQueue)
                .build();

        // Assert
        assertNotNull(transports.dispatcher());
        assertNotNull(transports.publisher());
    }

    @Test
    @DisplayName("Should build transports with custom executor service")
    void shouldBuildTransportsWithCustomExecutorService() {
        // Act
        LocalQueueTransportsBuilder.InMemoryTransports transports = new LocalQueueTransportsBuilder()
                .executorService(testExecutor)
                .build();

        // Assert
        assertNotNull(transports.dispatcher());
        assertNotNull(transports.publisher());
    }

    @Test
    @DisplayName("Should share the same queue between dispatcher and publisher transports")
    void shouldShareSameQueueBetweenTransports() throws InterruptedException {
        // Arrange
        LocalQueueTransportsBuilder.InMemoryTransports transports = new LocalQueueTransportsBuilder()
                .queueSize(10)
                .build();
        AtomicInteger receivedValue = new AtomicInteger(0);

        // Act
        transports.dispatcher().start(e -> receivedValue.set(((TestEvent) e).getValue()));
        Thread.sleep(50);

        TestEvent event = new TestEvent(42);
        transports.publisher().send(event);
        Thread.sleep(100);

        // Assert
        assertEquals(42, receivedValue.get(), "Event should be delivered through shared queue");

        transports.dispatcher().stop();
    }

    @Test
    @DisplayName("Should deliver multiple events through shared queue")
    void shouldDeliverMultipleEventsThroughSharedQueue() throws InterruptedException {
        // Arrange
        LocalQueueTransportsBuilder.InMemoryTransports transports = new LocalQueueTransportsBuilder()
                .queueSize(10)
                .build();
        java.util.List<Integer> deliveredValues = new java.util.concurrent.CopyOnWriteArrayList<>();

        // Act
        transports.dispatcher().start(e -> deliveredValues.add(((TestEvent) e).getValue()));
        Thread.sleep(50);

        transports.publisher().send(new TestEvent(1));
        transports.publisher().send(new TestEvent(2));
        transports.publisher().send(new TestEvent(3));
        Thread.sleep(200);

        // Assert
        assertEquals(3, deliveredValues.size());
        assertEquals(1, deliveredValues.get(0));
        assertEquals(2, deliveredValues.get(1));
        assertEquals(3, deliveredValues.get(2));

        transports.dispatcher().stop();
    }

    @Test
    @DisplayName("Should reject events when shared queue is full")
    void shouldRejectEventsWhenSharedQueueIsFull() {
        // Arrange
        LocalQueueTransportsBuilder.InMemoryTransports transports = new LocalQueueTransportsBuilder()
                .queueSize(2)
                .build();

        // Act
        transports.publisher().send(new TestEvent(1));
        transports.publisher().send(new TestEvent(2));

        // Assert
        assertThrows(TransportException.class, () ->
                transports.publisher().send(new TestEvent(3))
        );
    }

    @Test
    @DisplayName("Should build multiple independent transport pairs")
    void shouldBuildMultipleIndependentTransportPairs() throws InterruptedException {
        // Arrange
        LocalQueueTransportsBuilder.InMemoryTransports transports1 = new LocalQueueTransportsBuilder()
                .queueSize(10)
                .build();
        LocalQueueTransportsBuilder.InMemoryTransports transports2 = new LocalQueueTransportsBuilder()
                .queueSize(10)
                .build();

        AtomicInteger receivedValue1 = new AtomicInteger(0);
        AtomicInteger receivedValue2 = new AtomicInteger(0);

        transports1.dispatcher().start(e -> receivedValue1.set(((TestEvent) e).getValue()));
        transports2.dispatcher().start(e -> receivedValue2.set(((TestEvent) e).getValue()));
        Thread.sleep(50);

        // Act - send event to first transport pair
        transports1.publisher().send(new TestEvent(100));
        Thread.sleep(100);

        // Assert - only first transport should receive the event
        assertEquals(100, receivedValue1.get());
        assertEquals(0, receivedValue2.get());

        transports1.dispatcher().stop();
        transports2.dispatcher().stop();
    }

    /**
     * Test event class.
     */
    private static class TestEvent extends AbstractTraceableEvent {
        private final int value;

        TestEvent(int value) {
            super();
            this.value = value;
        }

        int getValue() {
            return value;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"value\":" + value + ",\"timestamp\":\"" + LocalDateTime.now() + "\"}";
        }
    }
}
