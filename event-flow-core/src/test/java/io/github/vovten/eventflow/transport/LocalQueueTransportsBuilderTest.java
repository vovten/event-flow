package io.github.vovten.eventflow.transport;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link LocalQueueTransportsBuilder}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@DisplayName("LocalQueueTransportsBuilder Tests")
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
        LocalQueueTransportsBuilder.LocalQueueTransports transports = new LocalQueueTransportsBuilder()
                .build();

        // Assert
        assertNotNull(transports.dispatcher());
        assertNotNull(transports.publisher());
        assertEquals("local-queue", transports.dispatcher().name());
        assertEquals("local-queue", transports.publisher().name());
    }

    @Test
    @DisplayName("Should build transports with custom queue size")
    void shouldBuildTransportsWithCustomQueueSize() {
        // Act
        LocalQueueTransportsBuilder.LocalQueueTransports transports = new LocalQueueTransportsBuilder()
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
        LocalQueueTransportsBuilder.LocalQueueTransports transports = new LocalQueueTransportsBuilder()
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
        LocalQueueTransportsBuilder.LocalQueueTransports transports = new LocalQueueTransportsBuilder()
                .executorService(testExecutor)
                .build();

        // Assert
        assertNotNull(transports.dispatcher());
        assertNotNull(transports.publisher());
    }

    @Test
    @DisplayName("Should share the same queue between dispatcher and publisher transports")
    void shouldShareSameQueueBetweenTransports() {
        // Arrange
        LocalQueueTransportsBuilder.LocalQueueTransports transports = new LocalQueueTransportsBuilder()
                .queueSize(10)
                .build();
        AtomicInteger receivedValue = new AtomicInteger(0);

        // Act
        transports.dispatcher().start(e -> receivedValue.set(((TestEvent) e).getValue()));

        TestEvent event = new TestEvent(42);
        transports.publisher().send(event);
        await().atMost(Duration.ofSeconds(2)).until(() -> receivedValue.get() == 42);

        // Assert
        assertThat(receivedValue.get()).as("Event should be delivered through shared queue").isEqualTo(42);

        transports.dispatcher().stop();
    }

    @Test
    @DisplayName("Should deliver multiple events through shared queue")
    void shouldDeliverMultipleEventsThroughSharedQueue() {
        // Arrange
        LocalQueueTransportsBuilder.LocalQueueTransports transports = new LocalQueueTransportsBuilder()
                .queueSize(10)
                .build();
        java.util.List<Integer> deliveredValues = new java.util.concurrent.CopyOnWriteArrayList<>();

        // Act
        transports.dispatcher().start(e -> deliveredValues.add(((TestEvent) e).getValue()));

        transports.publisher().send(new TestEvent(1));
        transports.publisher().send(new TestEvent(2));
        transports.publisher().send(new TestEvent(3));
        await().atMost(Duration.ofSeconds(2)).until(() -> deliveredValues.size() == 3);

        // Assert
        assertThat(deliveredValues.get(0)).isEqualTo(1);
        assertThat(deliveredValues.get(1)).isEqualTo(2);
        assertThat(deliveredValues.get(2)).isEqualTo(3);

        transports.dispatcher().stop();
    }

    @Test
    @DisplayName("Should return failed result when shared queue is full")
    void shouldReturnFailedResultWhenSharedQueueIsFull() {
        // Arrange
        LocalQueueTransportsBuilder.LocalQueueTransports transports = new LocalQueueTransportsBuilder()
                .queueSize(2)
                .build();

        // Act
        transports.publisher().send(new TestEvent(1));
        transports.publisher().send(new TestEvent(2));
        CompletableFuture<SendResult> future = transports.publisher().send(new TestEvent(3));

        // Assert
        SendResult result = future.join();
        assertFalse(result.success());
        assertTrue(result.errorDetails().contains("Queue is full"));
    }

    @Test
    @DisplayName("Should build multiple independent transport pairs")
    void shouldBuildMultipleIndependentTransportPairs() {
        // Arrange
        LocalQueueTransportsBuilder.LocalQueueTransports transports1 = new LocalQueueTransportsBuilder()
                .queueSize(10)
                .build();
        LocalQueueTransportsBuilder.LocalQueueTransports transports2 = new LocalQueueTransportsBuilder()
                .queueSize(10)
                .build();

        AtomicInteger receivedValue1 = new AtomicInteger(0);
        AtomicInteger receivedValue2 = new AtomicInteger(0);

        transports1.dispatcher().start(e -> receivedValue1.set(((TestEvent) e).getValue()));
        transports2.dispatcher().start(e -> receivedValue2.set(((TestEvent) e).getValue()));

        // Act - send event to first transport pair
        transports1.publisher().send(new TestEvent(100));
        await().atMost(Duration.ofSeconds(2)).until(() -> receivedValue1.get() == 100);

        // Assert - only first transport should receive the event
        assertThat(receivedValue1.get()).isEqualTo(100);
        assertThat(receivedValue2.get()).as("Second transport should not receive event").isEqualTo(0);

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
