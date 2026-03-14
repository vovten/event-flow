package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.EventHandler;
import com.github.vovten.eventflow.registry.EventHandlerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AbstractEventDispatcher}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("AbstractEventDispatcher Tests")
class AbstractEventDispatcherTest {

    private ExecutorService executorService;
    private TestEventDispatcher dispatcher;
    private TestEventHandlerRegistry handlerRegistry;

    @BeforeEach
    void setUp() {
        executorService = Executors.newSingleThreadExecutor();
        handlerRegistry = new TestEventHandlerRegistry();
        dispatcher = new TestEventDispatcher(executorService, handlerRegistry);
    }

    @Test
    @DisplayName("Should dispatch event to handler")
    void shouldDispatchEventToHandler() throws Exception {
        // Arrange
        TestEvent event = new TestEvent("test-data");
        TestEventHandler handler = new TestEventHandler();
        handlerRegistry.addHandler(handler);

        // Act
        dispatcher.dispatch(event);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        // Assert
        assertTrue(handler.called);
        assertEquals(event, handler.receivedEvent);
    }

    @Test
    @DisplayName("Should not dispatch when no handlers")
    void shouldNotDispatchWhenNoHandlers() throws Exception {
        // Arrange
        TestEvent event = new TestEvent("test-data");

        // Act
        dispatcher.dispatch(event);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        // Assert - should complete without errors
        assertTrue(executorService.awaitTermination(1, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Should handle multiple handlers")
    void shouldHandleMultipleHandlers() throws Exception {
        // Arrange
        TestEvent event = new TestEvent("test-data");
        TestEventHandler handler1 = new TestEventHandler();
        TestEventHandler handler2 = new TestEventHandler();
        handlerRegistry.addHandler(handler1);
        handlerRegistry.addHandler(handler2);

        // Act
        dispatcher.dispatch(event);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        // Assert
        assertTrue(handler1.called);
        assertTrue(handler2.called);
    }

    @Test
    @DisplayName("Should register handler")
    void shouldRegisterHandler() {
        // Arrange
        Object handler = new Object();

        // Act
        dispatcher.register(handler);

        // Assert
        assertTrue(handlerRegistry.registeredListeners.contains(handler));
    }

    @Test
    @DisplayName("Should check if handler is registered")
    void shouldCheckIfHandlerIsRegistered() {
        // Arrange
        Object handler = new Object();
        handlerRegistry.registeredListeners.add(handler);

        // Act & Assert
        assertTrue(dispatcher.isRegistered(handler));
    }

    /**
     * Test event.
     */
    private static final class TestEvent extends AbstractTraceableEvent {
        private final String data;
        private final LocalDateTime timestamp;

        TestEvent(String data) {
            super();
            this.data = data;
            this.timestamp = LocalDateTime.now();
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\",\"timestamp\":\"" + timestamp + "\"}";
        }
    }

    /**
     * Test event handler.
     */
    private static final class TestEventHandler {
        private boolean called = false;
        private TestEvent receivedEvent;

        void onEvent(TestEvent event) {
            this.called = true;
            this.receivedEvent = event;
        }
    }

    /**
     * Test handler registry.
     */
    private static final class TestEventHandlerRegistry implements EventHandlerRegistry {
        private final List<Object> registeredListeners = new ArrayList<>();
        private final List<TestEventHandler> handlers = new ArrayList<>();

        void addHandler(TestEventHandler handler) {
            Class<? extends Event> eventType = TestEvent.class;
            this.handlers.add(handler);
        }

        @Override
        public List<EventHandler> getHandlers(Event event) {
            List<EventHandler> result = new ArrayList<>();
            for (TestEventHandler handler : handlers) {
                result.add(new SimpleEventHandler(handler));
            }
            return result;
        }

        @Override
        public int handlerCount() {
            return handlers.size();
        }

        @Override
        public void register(Object handler) {
            registeredListeners.add(handler);
        }

        @Override
        public boolean unregister(Object handler) {
            return registeredListeners.remove(handler);
        }

        @Override
        public boolean isRegistered(Object handler) {
            return registeredListeners.contains(handler);
        }

        @Override
        public void merge(EventHandlerRegistry registry) {
        }

        @Override
        public String name() {
            return "test";
        }

        private static class SimpleEventHandler implements EventHandler {
            private final TestEventHandler delegate;

            SimpleEventHandler(TestEventHandler delegate) {
                this.delegate = delegate;
            }

            @Override
            public void onEvent(Event event) {
                delegate.onEvent((TestEvent) event);
            }
        }
    }

    /**
     * Test dispatcher.
     */
    private static final class TestEventDispatcher extends AbstractEventDispatcher {
        TestEventDispatcher(ExecutorService executorService, EventHandlerRegistry handlerRegistry) {
            super(executorService, handlerRegistry);
        }

        @Override
        public void start() {
            // No-op for tests
        }

        @Override
        public void stop() {
            // No-op for tests
        }
    }
}
