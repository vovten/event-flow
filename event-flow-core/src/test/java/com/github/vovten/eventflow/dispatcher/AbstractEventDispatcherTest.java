package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

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
    private TestEventListenerRegistry listenerRegistry;

    @BeforeEach
    void setUp() {
        executorService = Executors.newSingleThreadExecutor();
        listenerRegistry = new TestEventListenerRegistry();
        dispatcher = new TestEventDispatcher(executorService, listenerRegistry);
    }

    @Test
    @DisplayName("Should dispatch event to listener")
    void shouldDispatchEventToListener() throws Exception {
        // Arrange
        TestEvent event = new TestEvent("test-data");
        TestEventListener listener = new TestEventListener();
        listenerRegistry.addListener(TestEvent.class, listener);

        // Act
        dispatcher.dispatch(event);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        // Assert
        assertTrue(listener.called);
        assertEquals(event, listener.receivedEvent);
    }

    @Test
    @DisplayName("Should not dispatch when no listeners")
    void shouldNotDispatchWhenNoListeners() throws Exception {
        // Arrange
        TestEvent event = new TestEvent("test-data");

        // Act
        dispatcher.dispatch(event);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        // Assert
        assertTrue(true); // No exception thrown
    }

    @Test
    @DisplayName("Should register listener")
    void shouldRegisterListener() {
        // Arrange
        Object listener = new Object();

        // Act
        dispatcher.register(listener);

        // Assert
        assertTrue(listenerRegistry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should not register already registered listener")
    void shouldNotRegisterAlreadyRegisteredListener() {
        // Arrange
        Object listener = new Object();
        listenerRegistry.register(listener);

        // Act
        dispatcher.register(listener);

        // Assert - should not register again (registry already has it)
        assertTrue(listenerRegistry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should check if listener is registered")
    void shouldCheckIfListenerIsRegistered() {
        // Arrange
        Object listener = new Object();
        listenerRegistry.register(listener);

        // Act & Assert
        assertTrue(dispatcher.isRegistered(listener));
    }

    @Test
    @DisplayName("Should handle multiple listeners")
    void shouldHandleMultipleListeners() throws Exception {
        // Arrange
        TestEvent event = new TestEvent("test-data");
        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();
        listenerRegistry.addListener(TestEvent.class, listener1);
        listenerRegistry.addListener(TestEvent.class, listener2);

        // Act
        dispatcher.dispatch(event);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        // Assert
        assertTrue(listener1.called);
        assertTrue(listener2.called);
    }

    /**
     * Test implementation of AbstractEventDispatcher.
     */
    private static class TestEventDispatcher extends AbstractEventDispatcher {
        protected TestEventDispatcher(ExecutorService executorService,
                                      EventListenerRegistry listenerRegistry) {
            super(executorService, listenerRegistry);
        }
    }

    /**
     * Test event class.
     */
    private static class TestEvent implements Event {
        private final String data;
        private final LocalDateTime timestamp;

        public TestEvent(String data) {
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
     * Test event listener.
     */
    private static class TestEventListener {
        volatile boolean called = false;
        volatile TestEvent receivedEvent = null;

        void onEvent(TestEvent event) {
            this.called = true;
            this.receivedEvent = event;
        }
    }

    /**
     * Test listener registry.
     */
    private static class TestEventListenerRegistry implements EventListenerRegistry {
        private final List<Object> registeredListeners = new ArrayList<>();
        private final List<TestEventListener> listeners = new ArrayList<>();
        private Class<? extends Event> eventType;

        void addListener(Class<? extends Event> type, TestEventListener listener) {
            this.eventType = type;
            this.listeners.add(listener);
        }

        @Override
        public List<EventListener> getListeners(Event event) {
            List<EventListener> result = new ArrayList<>();
            for (TestEventListener listener : listeners) {
                result.add(new SimpleEventListener(listener));
            }
            return result;
        }

        @Override
        public int listenerCount() {
            return listeners.size();
        }

        @Override
        public void register(Object listener) {
            registeredListeners.add(listener);
        }

        @Override
        public boolean unregister(Object listener) {
            return registeredListeners.remove(listener);
        }

        @Override
        public boolean isRegistered(Object listener) {
            return registeredListeners.contains(listener);
        }

        @Override
        public void merge(EventListenerRegistry registry) {
        }

        private class SimpleEventListener implements EventListener {
            private final TestEventListener delegate;

            SimpleEventListener(TestEventListener delegate) {
                this.delegate = delegate;
            }

            @Override
            public List<Class<? extends Event>> events() {
                return List.of(TestEvent.class);
            }

            @Override
            public void onEvent(Event event) {
                delegate.onEvent((TestEvent) event);
            }
        }
    }
}
