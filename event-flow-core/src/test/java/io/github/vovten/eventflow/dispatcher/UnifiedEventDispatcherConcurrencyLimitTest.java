package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.registry.EventSubscriberRegistry;
import io.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for concurrency limiting in UnifiedEventDispatcher.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@DisplayName("UnifiedEventDispatcher Concurrency Limit Tests")
class UnifiedEventDispatcherConcurrencyLimitTest {

    private ExecutorService executorService;
    private ExecutorService dispatcherExecutor;
    private EventHandlerRegistry handlerRegistry;
    private InTransport transport;

    @BeforeEach
    void setUp() {
        executorService = Executors.newVirtualThreadPerTaskExecutor();
        dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
        handlerRegistry = new EventSubscriberRegistry();
        transport = mock(InTransport.class);
        when(transport.name()).thenReturn("test-transport");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdownNow();
            dispatcherExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("Should limit concurrent handler executions")
    void shouldLimitConcurrentHandlerExecutions() throws InterruptedException {
        int concurrencyLimit = 2;
        int numEvents = 5;
        int handlerSleepMs = 100;

        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger currentConcurrent = new AtomicInteger(0);
        CountDownLatch allHandlersStarted = new CountDownLatch(numEvents);

        SlowHandler handler = new SlowHandler(handlerSleepMs, currentConcurrent, maxConcurrent, allHandlersStarted);
        handlerRegistry.register(handler);

        Semaphore semaphore = new Semaphore(concurrencyLimit);
        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                executorService, handlerRegistry, List.of(transport), semaphore);

        doAnswer(invocation -> {
            Consumer<Event> consumer = invocation.getArgument(0);
            for (int i = 0; i < numEvents; i++) {
                consumer.accept(new ConcurrencyTestEvent(i));
            }
            return null;
        }).when(transport).start(any(Consumer.class));

        dispatcher.start(dispatcher::dispatch);

        boolean completed = allHandlersStarted.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "All handlers should have been invoked");

        dispatcher.stop();

        // Verify that concurrency never exceeded the limit
        assertTrue(maxConcurrent.get() <= concurrencyLimit,
                "Max concurrent executions (" + maxConcurrent.get() +
                        ") should not exceed limit (" + concurrencyLimit + ")");
    }

    @Test
    @DisplayName("Should work without semaphore (unlimited concurrency)")
    void shouldWorkWithoutSemaphore() throws InterruptedException {
        int numEvents = 5;
        int handlerSleepMs = 50;

        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger currentConcurrent = new AtomicInteger(0);
        CountDownLatch allHandlersStarted = new CountDownLatch(numEvents);

        SlowHandler handler = new SlowHandler(handlerSleepMs, currentConcurrent, maxConcurrent, allHandlersStarted);
        handlerRegistry.register(handler);

        // No semaphore -- null
        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                executorService, handlerRegistry, List.of(transport), null);

        doAnswer(invocation -> {
            Consumer<Event> consumer = invocation.getArgument(0);
            for (int i = 0; i < numEvents; i++) {
                consumer.accept(new ConcurrencyTestEvent(i));
            }
            return null;
        }).when(transport).start(any(Consumer.class));

        dispatcher.start(dispatcher::dispatch);

        boolean completed = allHandlersStarted.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "All handlers should complete without semaphore");

        dispatcher.stop();

        // Without limit, all handlers can run concurrently
        assertEquals(numEvents, maxConcurrent.get(),
                "Without limit, all handlers should run concurrently");
    }

    @Test
    @DisplayName("Should build dispatcher with concurrency limit via builder")
    void shouldBuildDispatcherWithConcurrencyLimit() {
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of(transport))
                .concurrencyLimit(50)
                .build();

        assertNotNull(dispatcher);
        assertInstanceOf(UnifiedEventDispatcher.class, dispatcher);
    }

    @Test
    @DisplayName("Should throw when concurrency limit is zero")
    void shouldThrowWhenConcurrencyLimitIsZero() {
        assertThrows(IllegalArgumentException.class, () ->
                EventDispatcherBuilder.create()
                        .executor(executorService)
                        .handlerRegistry(handlerRegistry)
                        .transports(List.of(transport))
                        .concurrencyLimit(0)
        );
    }

    @Test
    @DisplayName("Should throw when concurrency limit is negative")
    void shouldThrowWhenConcurrencyLimitIsNegative() {
        assertThrows(IllegalArgumentException.class, () ->
                EventDispatcherBuilder.create()
                        .executor(executorService)
                        .handlerRegistry(handlerRegistry)
                        .transports(List.of(transport))
                        .concurrencyLimit(-5)
        );
    }

    // ==================== Test Helpers ====================

    static class ConcurrencyTestEvent extends AbstractTraceableEvent {
        private final int index;

        ConcurrencyTestEvent(int index) {
            super();
            this.index = index;
        }

        @Override
        public Class<? extends Event> type() {
            return ConcurrencyTestEvent.class;
        }
    }

    static class SlowHandler implements EventSubscriber {
        private final int sleepMs;
        private final AtomicInteger currentConcurrent;
        private final AtomicInteger maxConcurrent;
        private final CountDownLatch latch;

        SlowHandler(int sleepMs, AtomicInteger currentConcurrent, AtomicInteger maxConcurrent, CountDownLatch latch) {
            this.sleepMs = sleepMs;
            this.currentConcurrent = currentConcurrent;
            this.maxConcurrent = maxConcurrent;
            this.latch = latch;
        }

        @Override
        public List<Class<?>> events() {
            return List.of(ConcurrencyTestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            int current = currentConcurrent.incrementAndGet();
            try {
                maxConcurrent.accumulateAndGet(current, Math::max);
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                currentConcurrent.decrementAndGet();
                latch.countDown();
            }
        }
    }
}
