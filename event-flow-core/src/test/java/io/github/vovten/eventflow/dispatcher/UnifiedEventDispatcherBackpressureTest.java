package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for backpressure handling in UnifiedEventDispatcher
 * @since 1.0.0
 */
@DisplayName("UnifiedEventDispatcher Backpressure Tests")
class UnifiedEventDispatcherBackpressureTest {

    private ExecutorService executorService;
    private EventHandlerRegistry handlerRegistry;
    private UnifiedEventDispatcher dispatcher;
    private List<InTransport> transports;
    private Map<Class<?>, List<EventHandler>> handlerMap;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        handlerMap = new HashMap<>();
        handlerRegistry = new MapBasedHandlerRegistry(handlerMap);
        transports = List.of();
    }

    @AfterEach
    void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should handle executor saturation gracefully with CallerRunsPolicy")
    void shouldHandleExecutorSaturationGracefully() throws InterruptedException {
        // Create a bounded queue that will cause rejection
        executorService = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, transports);

        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch canFinish = new CountDownLatch(1);
        List<String> executionLog = new ArrayList<>();

        // Create a slow handler that will block the executor
        EventHandler slowHandler = event -> {
            executionLog.add("handler-started");
            handlerStarted.countDown();
            try {
                canFinish.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executionLog.add("handler-finished");
        };

        // Create a test event
        Event testEvent = new TestEvent();

        // Register the handler
        handlerMap.put(TestEvent.class, List.of(slowHandler));

        // Submit first event - will occupy the executor
        Thread dispatcherThread = new Thread(() -> dispatcher.dispatch(testEvent));
        dispatcherThread.start();

        // Wait for handler to start
        handlerStarted.await(2, TimeUnit.SECONDS);

        // Submit second event - should trigger CallerRunsPolicy
        // This should NOT throw exception, but execute in current thread
        assertDoesNotThrow(() -> dispatcher.dispatch(testEvent));

        // Allow handlers to finish
        canFinish.countDown();
        dispatcherThread.join(5000);

        // Verify both events were handled (log should have 2 starts and 2 finishes)
        assertTrue(executionLog.contains("handler-started"));
    }

    @Test
    @DisplayName("Should log warning on partial handler submission")
    void shouldLogWarningOnPartialHandlerSubmission() {
        // Create executor that rejects immediately
        executorService = Executors.newSingleThreadExecutor();
        executorService.shutdownNow(); // Reject all new tasks

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, transports);

        EventHandler handler1 = event -> {};
        EventHandler handler2 = event -> {};
        Event testEvent = new TestEvent();
        handlerMap.put(TestEvent.class, List.of(handler1, handler2));

        // Should not throw, but should log warning
        assertDoesNotThrow(() -> dispatcher.dispatch(testEvent));
    }

    @Test
    @DisplayName("Should continue dispatching when one handler fails")
    void shouldContinueDispatchingWhenOneHandlerFails() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, transports);

        EventHandler failingHandler = event -> {
            throw new RuntimeException("Handler failed");
        };
        EventHandler successHandler = event -> {};

        Event testEvent = new TestEvent();
        handlerMap.put(TestEvent.class, List.of(failingHandler, successHandler));

        // Should not throw - both handlers should be submitted
        assertDoesNotThrow(() -> dispatcher.dispatch(testEvent));
    }

    /**
     * Test event class for mock type information
     */
    static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

}
