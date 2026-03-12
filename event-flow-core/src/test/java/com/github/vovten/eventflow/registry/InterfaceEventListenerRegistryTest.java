package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link InterfaceEventListenerRegistry}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("InterfaceEventListenerRegistry Tests")
class InterfaceEventListenerRegistryTest {

    private InterfaceEventListenerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new InterfaceEventListenerRegistry();
    }

    @Test
    @DisplayName("Should register listener for specific event type")
    void shouldRegisterListenerForSpecificEventType() {
        // Arrange
        TestEventListener listener = new TestEventListener();

        // Act
        registry.register(listener);

        // Assert
        List<EventListener> listeners = registry.getListeners(new TestEvent("test"));
        assertEquals(1, listeners.size());
        assertTrue(listeners.contains(listener));
    }

    @Test
    @DisplayName("Should register listener for multiple event types")
    void shouldRegisterListenerForMultipleEventTypes() {
        // Arrange
        MultiEventListener listener = new MultiEventListener();

        // Act
        registry.register(listener);

        // Assert
        List<EventListener> testListeners = registry.getListeners(new TestEvent("test"));
        List<EventListener> anotherListeners = registry.getListeners(new AnotherEvent("test"));
        assertTrue(testListeners.contains(listener));
        assertTrue(anotherListeners.contains(listener));
    }

    @Test
    @DisplayName("Should register generic listener for all events")
    void shouldRegisterGenericListenerForAllEvents() {
        // Arrange
        GenericEventListener genericListener = new GenericEventListener();

        // Act
        registry.register(genericListener);

        // Assert
        List<EventListener> listeners = registry.getListeners(new TestEvent("test"));
        assertEquals(1, listeners.size());
    }

    @Test
    @DisplayName("Should return empty list when no listeners")
    void shouldReturnEmptyListWhenNoListeners() {
        // Act
        List<EventListener> listeners = registry.getListeners(new TestEvent("test"));

        // Assert
        assertTrue(listeners.isEmpty());
    }

    @Test
    @DisplayName("Should count unique event types")
    void shouldCountUniqueEventTypes() {
        // Arrange
        registry.register(new TestEventListener());
        registry.register(new AnotherEventListener());

        // Act
        int count = registry.listenerCount();

        // Assert
        assertEquals(2, count);
    }

    @Test
    @DisplayName("Should unregister listener")
    void shouldUnregisterListener() {
        // Arrange
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        // Act
        boolean unregistered = registry.unregister(listener);

        // Assert
        assertTrue(unregistered);
        assertTrue(registry.getListeners(new TestEvent("test")).isEmpty());
    }

    @Test
    @DisplayName("Should return false when unregistering non-existent listener")
    void shouldReturnFalseWhenUnregisteringNonExistentListener() {
        // Arrange
        TestEventListener listener = new TestEventListener();

        // Act
        boolean unregistered = registry.unregister(listener);

        // Assert
        assertFalse(unregistered);
    }

    @Test
    @DisplayName("Should check if listener is registered")
    void shouldCheckIfListenerIsRegistered() {
        // Arrange
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        // Act & Assert
        assertTrue(registry.isRegistered(listener));
        assertFalse(registry.isRegistered(new AnotherEventListener()));
    }

    @Test
    @DisplayName("Should throw exception for merge operation")
    void shouldThrowExceptionForMergeOperation() {
        // Arrange
        InterfaceEventListenerRegistry otherRegistry = new InterfaceEventListenerRegistry();

        // Assert
        assertThrows(UnsupportedOperationException.class, () ->
                registry.merge(otherRegistry)
        );
    }

    @Test
    @DisplayName("Should handle multiple listeners for same event type")
    void shouldHandleMultipleListenersForSameEventType() {
        // Arrange
        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();

        // Act
        registry.register(listener1);
        registry.register(listener2);

        // Assert
        List<EventListener> listeners = registry.getListeners(new TestEvent("test"));
        assertEquals(2, listeners.size());
    }

    /**
     * Test event class.
     */
    private static final class TestEvent implements Event {
        private final String data;

        TestEvent(String data) {
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\"}";
        }
    }

    /**
     * Another test event class.
     */
    private static final class AnotherEvent implements Event {
        private final String data;

        AnotherEvent(String data) {
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return AnotherEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\"}";
        }
    }

    /**
     * Test listener for specific event type.
     */
    private static final class TestEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    /**
     * Test listener for another event type.
     */
    private static final class AnotherEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(AnotherEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    /**
     * Test listener for multiple event types.
     */
    private static final class MultiEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class, AnotherEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    /**
     * Generic listener for all events.
     */
    private static final class GenericEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(Event.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
