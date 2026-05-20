package io.github.vovten.eventflow.transport.incoming;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for LocalQueueDispatcherTransport.
 * @since 1.0.0
 */
@DisplayName("LocalQueueDispatcherTransport Tests")
class LocalQueueInTransportTest {

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
    @DisplayName("Should create transport with queue and executor")
    void shouldCreateTransportWithQueueAndExecutor() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(100);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, executor);

        assertEquals("local-queue", transport.name());
    }

    @Test
    @DisplayName("Should deliver events when started")
    void shouldDeliverEventsWhenStarted() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, testExecutor);
        AtomicInteger deliveredCount = new AtomicInteger(0);
        AtomicInteger receivedValue = new AtomicInteger(0);

        TestEvent event = new TestEvent(42);
        transport.start(e -> {
            receivedValue.set(((TestEvent) e).getValue());
            deliveredCount.incrementAndGet();
        });

        queue.offer(event);

        await().atMost(Duration.ofSeconds(2)).until(() -> deliveredCount.get() == 1);
        assertThat(receivedValue.get()).as("Event value should be delivered").isEqualTo(42);

        transport.stop();
    }

    @Test
    @DisplayName("Should log warning when starting already running transport")
    void shouldLogWarningWhenStartingAlreadyRunningTransport() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, testExecutor);

        transport.start(e -> {});
        assertDoesNotThrow(() -> transport.start(e -> {}));

        transport.stop();
    }

    @Test
    @DisplayName("Should stop processing after stop is called")
    void shouldStopProcessingAfterStopIsCalled() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, testExecutor);
        AtomicInteger deliveredCount = new AtomicInteger(0);

        transport.start(e -> deliveredCount.incrementAndGet());

        queue.offer(new TestEvent(1));
        await().atMost(Duration.ofSeconds(2)).until(() -> deliveredCount.get() >= 1);

        transport.stop();

        queue.offer(new TestEvent(2));
        await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1))
                .until(() -> deliveredCount.get() == 1);

        assertThat(deliveredCount.get()).as("Only first event should be delivered").isEqualTo(1);
    }

    @Test
    @DisplayName("Should not throw when stop is called multiple times")
    void shouldNotThrowWhenStopIsCalledMultipleTimes() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, testExecutor);

        assertDoesNotThrow(() -> {
            transport.stop();
            transport.stop();
            transport.stop();
        });
    }

    @Test
    @DisplayName("Should do nothing when stop is called on non-running transport")
    void shouldDoNothingWhenStopIsCalledOnNonRunningTransport() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, testExecutor);

        assertDoesNotThrow(transport::stop);
    }

    @Test
    @DisplayName("Should deliver multiple events in order")
    void shouldDeliverMultipleEventsInOrder() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, testExecutor);
        java.util.List<Integer> deliveredValues = new java.util.concurrent.CopyOnWriteArrayList<>();

        transport.start(e -> deliveredValues.add(((TestEvent) e).getValue()));

        queue.offer(new TestEvent(1));
        queue.offer(new TestEvent(2));
        queue.offer(new TestEvent(3));

        await().atMost(Duration.ofSeconds(2)).until(() -> deliveredValues.size() == 3);

        assertThat(deliveredValues.get(0)).isEqualTo(1);
        assertThat(deliveredValues.get(1)).isEqualTo(2);
        assertThat(deliveredValues.get(2)).isEqualTo(3);

        transport.stop();
    }

    static class TestEvent extends AbstractTraceableEvent {
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
    }
}
