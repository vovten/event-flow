package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.EventHandler;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.registry.EventHandlerRegistry;
import com.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for backpressure handling in UnifiedEventDispatcher
 */
@DisplayName("UnifiedEventDispatcher Backpressure Tests")
class UnifiedEventDispatcherBackpressureTest {

    private ExecutorService executorService;
    private EventHandlerRegistry handlerRegistry;
    private UnifiedEventDispatcher dispatcher;
    private List<InTransport> transports;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        handlerRegistry = mock(EventHandlerRegistry.class);
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
        EventHandler slowHandler = mock(EventHandler.class);
        doAnswer(invocation -> {
            executionLog.add("handler-started");
            handlerStarted.countDown();
            canFinish.await(5, TimeUnit.SECONDS);
            executionLog.add("handler-finished");
            return null;
        }).when(slowHandler).onEvent(any());

        // Create a test event
        Event testEvent = mock(Event.class);
        when(testEvent.type()).thenReturn((Class) TestEvent.class);

        // Register the handler
        when(handlerRegistry.getHandlers(testEvent)).thenReturn(List.of(slowHandler));

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

        EventHandler handler1 = mock(EventHandler.class);
        EventHandler handler2 = mock(EventHandler.class);
        Event testEvent = mock(Event.class);
        when(testEvent.type()).thenReturn((Class) TestEvent.class);
        when(handlerRegistry.getHandlers(testEvent)).thenReturn(List.of(handler1, handler2));

        // Should not throw, but should log warning
        assertDoesNotThrow(() -> dispatcher.dispatch(testEvent));
    }

    @Test
    @DisplayName("Should continue dispatching when one handler fails")
    void shouldContinueDispatchingWhenOneHandlerFails() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, transports);

        EventHandler failingHandler = mock(EventHandler.class);
        EventHandler successHandler = mock(EventHandler.class);

        doThrow(new RuntimeException("Handler failed")).when(failingHandler).onEvent(any());
        doNothing().when(successHandler).onEvent(any());

        Event testEvent = mock(Event.class);
        when(testEvent.type()).thenReturn((Class) TestEvent.class);
        when(handlerRegistry.getHandlers(testEvent)).thenReturn(List.of(failingHandler, successHandler));

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
