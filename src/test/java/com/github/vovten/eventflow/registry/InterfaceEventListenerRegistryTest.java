package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InterfaceEventListenerRegistry.
 */
class InterfaceEventListenerRegistryTest {

    private InterfaceEventListenerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new InterfaceEventListenerRegistry();
    }

    @Test
    void testRegister_InterfaceListener() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        assertEquals(1, registry.listenerCount());
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    void testRegister_NonInterfaceListener_Ignored() {
        Object nonListener = new Object();
        registry.register(nonListener);

        assertEquals(0, registry.listenerCount());
        assertFalse(registry.isRegistered(nonListener));
    }

    @Test
    void testGetListeners_ReturnsListenersForEventType() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        TestEvent event = new TestEvent();
        List<EventListener> listeners = registry.getListeners(event);

        assertEquals(1, listeners.size());
        assertTrue(listeners.contains(listener));
    }

    @Test
    void testGetListeners_IncludesGenericListeners() {
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
    void testGetListeners_NoListeners_ReturnsEmptyList() {
        TestEvent event = new TestEvent();
        List<EventListener> listeners = registry.getListeners(event);

        assertTrue(listeners.isEmpty());
    }

    @Test
    void testUnregister_ExistingListener() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        boolean result = registry.unregister(listener);

        assertTrue(result);
        assertFalse(registry.isRegistered(listener));
        assertEquals(0, registry.listenerCount());
    }

    @Test
    void testUnregister_NonExistingListener() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        Object nonListener = new Object();
        boolean result = registry.unregister(nonListener);

        assertFalse(result);
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    void testUnregister_NonInterfaceListener() {
        Object nonListener = new Object();
        boolean result = registry.unregister(nonListener);

        assertFalse(result);
    }

    @Test
    void testIsRegistered_InterfaceListener() {
        TestEventListener listener = new TestEventListener();
        registry.register(listener);

        assertTrue(registry.isRegistered(listener));
    }

    @Test
    void testIsRegistered_NonInterfaceListener() {
        Object nonListener = new Object();
        assertFalse(registry.isRegistered(nonListener));
    }

    @Test
    void testMerge_ThrowsUnsupportedOperationException() {
        InterfaceEventListenerRegistry otherRegistry = new InterfaceEventListenerRegistry();

        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    void testRegister_MultipleEventTypes() {
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
    void testRegister_DuplicateListener_Ignored() {
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
