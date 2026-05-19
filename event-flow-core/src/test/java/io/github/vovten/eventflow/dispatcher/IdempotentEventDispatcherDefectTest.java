package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.transport.InTransport;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests that expose the TOCTOU race condition in IdempotentEventDispatcher.
 * <p>
 * Defect CRIT-3: The idempotent check (cache.getIfPresent) and store (cache.put)
 * are not atomic. Between the check and the put, a duplicate event can also
 * pass the check, resulting in duplicate processing.
 *
 * @since 1.1.0
 */
@DisplayName("IdempotentEventDispatcher Race Condition Tests")
class IdempotentEventDispatcherDefectTest {

    private ExecutorService executor;
    private EventHandlerRegistry handlerRegistry;
    private InTransport transport;
    private EventDispatcher baseDispatcher;
    private CountDownLatch handlerEnteredLatch;
    private CountDownLatch releaseLatch;

    @BeforeEach
    void setUp() {
        executor = Executors.newCachedThreadPool();
        handlerRegistry = mock(EventHandlerRegistry.class);
        transport = mock(InTransport.class);
        when(transport.name()).thenReturn("test");

        handlerEnteredLatch = new CountDownLatch(1);
        releaseLatch = new CountDownLatch(1);
    }

    /**
     * Defect: When two threads dispatch the SAME event (same UUID) concurrently,
     * both pass the idempotent check before either completes, resulting in
     * duplicate processing of an idempotent event.
     * <p>
     * The test uses a blocking handler to ensure both threads reach the
     * check simultaneously.
     */
    @Test
    @DisplayName("CRIT-3: Should not dispatch duplicate events under concurrent access")
    void shouldNotDispatchDuplicateEventsUnderConcurrentAccess() throws Exception {
        // Arrange: a handler that blocks until released
        AtomicInteger invocationCount = new AtomicInteger(0);
        EventHandler blockingHandler = new EventHandler() {
            @Override
            public void onEvent(Event event) {
                invocationCount.incrementAndGet();
                handlerEnteredLatch.countDown(); // signal: handler entered
                try {
                    releaseLatch.await(5, TimeUnit.SECONDS); // block until released
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public String name() {
                return "blocking-handler";
            }
        };

        when(handlerRegistry.getHandlers(any())).thenReturn(List.of(blockingHandler));

        // Create base dispatcher + idempotent wrapper
        baseDispatcher = new UnifiedEventDispatcher(executor, handlerRegistry, List.of(transport));
        IdempotentEventDispatcher idempotentDispatcher = new IdempotentEventDispatcher(
                baseDispatcher, Duration.ofMinutes(10), 10_000, false
        );

        // Create TWO identical events (same UUID)
        UUID fixedEventId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        TestTraceableEvent event1 = new TestTraceableEvent(fixedEventId);
        TestTraceableEvent event2 = new TestTraceableEvent(fixedEventId);

        // Act: dispatch both concurrently
        Thread thread1 = new Thread(() -> idempotentDispatcher.dispatch(event1));
        Thread thread2 = new Thread(() -> idempotentDispatcher.dispatch(event2));

        thread1.start();
        thread2.start();

        // Wait for at least one handler to enter the blocking section
        handlerEnteredLatch.await(3, TimeUnit.SECONDS);

        // Small delay to allow the second thread to also pass the idempotent check
        Thread.sleep(200);

        // Release the blocked handlers
        releaseLatch.countDown();

        thread1.join(3000);
        thread2.join(3000);

        // Assert: the handler should have been invoked only ONCE
        assertThat(invocationCount.get())
                .as("CRIT-3 FAILED: Handler was invoked %d times for duplicate events with " +
                        "same UUID=%s. IdempotentEventDispatcher has a TOCTOU race: " +
                        "cache.getIfPresent() check and cache.put() in thenApply() are " +
                        "not atomic, allowing duplicates when concurrent dispatches occur.",
                        invocationCount.get(), fixedEventId)
                .isEqualTo(1);
    }

    /**
     * Same defect but tested with CountDownLatch for precise timing.
     */
    @Test
    @DisplayName("CRIT-3: Should handle rapid duplicate dispatches correctly")
    void shouldHandleRapidDuplicateDispatches() throws Exception {
        // Arrange
        AtomicInteger invocationCount = new AtomicInteger(0);
        EventHandler countingHandler = new EventHandler() {
            @Override
            public void onEvent(Event event) {
                invocationCount.incrementAndGet();
            }

            @Override
            public String name() {
                return "counting-handler";
            }
        };

        when(handlerRegistry.getHandlers(any())).thenReturn(List.of(countingHandler));

        baseDispatcher = new UnifiedEventDispatcher(executor, handlerRegistry, List.of(transport));
        IdempotentEventDispatcher idempotentDispatcher = new IdempotentEventDispatcher(
                baseDispatcher, Duration.ofMinutes(10), 10_000, false
        );

        UUID fixedEventId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        TestTraceableEvent event = new TestTraceableEvent(fixedEventId);

        // Act: dispatch the same event rapidly multiple times
        // Using submit() instead of parallel threads to create a tight race
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            executor.submit(() -> {
                idempotentDispatcher.dispatch(event);
                latch.countDown();
            });
        }
        latch.await(5, TimeUnit.SECONDS);

        // Small wait for any in-flight handlers
        Thread.sleep(500);

        // Assert: should only be invoked ONCE
        assertThat(invocationCount.get())
                .as("CRIT-3 (rapid) FAILED: Handler was invoked %d times for 10 rapid dispatches " +
                        "of the same event UUID=%s. Idempotent filter is not atomic.",
                        invocationCount.get(), fixedEventId)
                .isEqualTo(1);
    }

    /**
     * A TraceableEvent with a FIXED eventId (not random).
     */
    static class TestTraceableEvent implements TraceableEvent {
        private final UUID eventId;

        TestTraceableEvent(UUID eventId) {
            this.eventId = eventId;
        }

        @Override
        public UUID eventId() {
            return eventId;
        }

        @Override
        public UUID processId() {
            return null;
        }

        @Override
        public Instant occurredAt() {
            return Instant.now();
        }

        @Override
        public Class<? extends Event> type() {
            return TestTraceableEvent.class;
        }
    }
}
