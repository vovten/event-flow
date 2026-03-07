package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InterfaceEventListenerRegistry.
 */
@DisplayName("InterfaceEventListenerRegistry Tests")
class InterfaceEventListenerRegistryTest {

    private InterfaceEventListenerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new InterfaceEventListenerRegistry();
    }

    @Test
    @DisplayName("Should register interface listener")
    void shouldRegisterInterfaceListener() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        assertEquals(1, registry.listenerCount());
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should ignore non-interface listener")
    void shouldIgnoreNonInterfaceListener() {
        Object nonListener = new Object();
        registry.register(nonListener);

        assertEquals(0, registry.listenerCount());
        assertFalse(registry.isRegistered(nonListener));
    }

    @Test
    @DisplayName("Should return listeners for event type")
    void shouldReturnListenersForEventType() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        TestEvent event = new TestEvent();
        List<EventListener> listeners = registry.getListeners(event);

        assertEquals(1, listeners.size());
        assertTrue(listeners.contains(listener));
    }

    @Test
    @DisplayName("Should include generic listeners")
    void shouldIncludeGenericListeners() {
        SpecificEventListener specificListener = new SpecificEventListener();
        GenericEventListener genericListener = new GenericEventListener();

        registry.register(specificListener);
        registry.register(genericListener);

        SpecificEvent event = new SpecificEvent();
        List<EventListener> listeners = registry.getListeners(event);

        assertEquals(2, listeners.size());
        assertTrue(listeners.contains(specificListener));
        assertTrue(listeners.contains(genericListener));
    }

    @Test
    @DisplayName("Should return empty list when no listeners")
    void shouldReturnEmptyListWhenNoListeners() {
        TestEvent event = new TestEvent();
        List<EventListener> listeners = registry.getListeners(event);

        assertTrue(listeners.isEmpty());
    }

    @Test
    @DisplayName("Should unregister existing listener")
    void shouldUnregisterExistingListener() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        boolean result = registry.unregister(listener);

        assertTrue(result);
        assertFalse(registry.isRegistered(listener));
        List<EventListener> listeners = registry.getListeners(new TestEvent());
        assertTrue(listeners.isEmpty());
    }

    @Test
    @DisplayName("Should return false when unregistering non-existing listener")
    void shouldReturnFalseWhenUnregisteringNonExistingListener() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        Object nonListener = new Object();
        boolean result = registry.unregister(nonListener);

        assertFalse(result);
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should return false when unregistering non-interface listener")
    void shouldReturnFalseWhenUnregisteringNonInterfaceListener() {
        Object nonListener = new Object();
        boolean result = registry.unregister(nonListener);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should return true for registered interface listener")
    void shouldReturnTrueForRegisteredInterfaceListener() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should return false for non-interface listener")
    void shouldReturnFalseForNonInterfaceListener() {
        Object nonListener = new Object();
        assertFalse(registry.isRegistered(nonListener));
    }

    @Test
    @DisplayName("Should throw exception on merge")
    void shouldThrowExceptionOnMerge() {
        InterfaceEventListenerRegistry otherRegistry = new InterfaceEventListenerRegistry();

        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    @DisplayName("Should register listener for multiple event types")
    void shouldRegisterListenerForMultipleEventTypes() {
        MultiEventListener listener = new MultiEventListener();
        registry.register(listener);

        assertEquals(2, registry.listenerCount());

        List<EventListener> listeners1 = registry.getListeners(new TestEvent());
        List<EventListener> listeners2 = registry.getListeners(new SpecificEvent());

        assertEquals(1, listeners1.size());
        assertEquals(1, listeners2.size());
        assertTrue(listeners1.contains(listener));
        assertTrue(listeners2.contains(listener));
    }

    @Test
    @DisplayName("Should ignore duplicate listener registration")
    void shouldIgnoreDuplicateListenerRegistration() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);
        registry.register(listener);

        assertEquals(1, registry.listenerCount());
        List<EventListener> listeners = registry.getListeners(new TestEvent());
        assertEquals(1, listeners.size());
    }

    static class TestEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    static class SpecificEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(SpecificEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    static class SpecificEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return SpecificEvent.class;
        }
    }

    static class GenericEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(Event.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    static class MultiEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class, SpecificEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
