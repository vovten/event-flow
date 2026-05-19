package io.github.vovten.eventflow.transport.incoming;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

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
    void shouldDeliverEventsWhenStarted() throws InterruptedException {
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
        Thread.sleep(100);

        assertEquals(42, receivedValue.get(), "Event value should be delivered");
        assertEquals(1, deliveredCount.get());

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
    void shouldStopProcessingAfterStopIsCalled() throws InterruptedException {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, testExecutor);
        AtomicInteger deliveredCount = new AtomicInteger(0);

        transport.start(e -> deliveredCount.incrementAndGet());
        Thread.sleep(50);

        queue.offer(new TestEvent(1));
        Thread.sleep(100);

        transport.stop();
        Thread.sleep(50);
        queue.offer(new TestEvent(2));
        Thread.sleep(100);

        assertEquals(1, deliveredCount.get(), "Only first event should be delivered");
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
    void shouldDeliverMultipleEventsInOrder() throws InterruptedException {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        LocalQueueInTransport transport = new LocalQueueInTransport(queue, testExecutor);
        java.util.List<Integer> deliveredValues = new java.util.concurrent.CopyOnWriteArrayList<>();

        transport.start(e -> deliveredValues.add(((TestEvent) e).getValue()));

        queue.offer(new TestEvent(1));
        queue.offer(new TestEvent(2));
        queue.offer(new TestEvent(3));

        Thread.sleep(200);

        assertEquals(3, deliveredValues.size());
        assertEquals(1, deliveredValues.get(0));
        assertEquals(2, deliveredValues.get(1));
        assertEquals(3, deliveredValues.get(2));

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
