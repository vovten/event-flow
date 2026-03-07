package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for CompositeEventListenerRegistry.
 */
class CompositeEventListenerRegistryTest {

    private EventListenerRegistry registry1;
    private EventListenerRegistry registry2;
    private CompositeEventListenerRegistry compositeRegistry;

    @BeforeEach
    void setUp() {
        registry1 = mock(EventListenerRegistry.class);
        registry2 = mock(EventListenerRegistry.class);
        compositeRegistry = new CompositeEventListenerRegistry(List.of(registry1, registry2));
    }

    @Test
    void testConstructor_EmptyList() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeEventListenerRegistry(List.of()));
    }

    @Test
    void testConstructor_NullList() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeEventListenerRegistry(null));
    }

    @Test
    void testConstructor_WithRegistries() {
        assertDoesNotThrow(() -> new CompositeEventListenerRegistry(List.of(registry1)));
    }

    @Test
    void testGetListeners_CombinesFromAllRegistries() {
        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();

        when(registry1.getListeners(any())).thenReturn(List.of(listener1));
        when(registry2.getListeners(any())).thenReturn(List.of(listener2));

        TestEvent event = new TestEvent();
        List<EventListener> listeners = compositeRegistry.getListeners(event);

        assertEquals(2, listeners.size());
        assertTrue(listeners.contains(listener1));
        assertTrue(listeners.contains(listener2));
    }

    @Test
    void testGetListeners_EmptyRegistries() {
        when(registry1.getListeners(any())).thenReturn(List.of());
        when(registry2.getListeners(any())).thenReturn(List.of());

        TestEvent event = new TestEvent();
        List<EventListener> listeners = compositeRegistry.getListeners(event);

        assertTrue(listeners.isEmpty());
    }

    @Test
    void testListenerCount_SumsAllRegistries() {
        when(registry1.listenerCount()).thenReturn(5);
        when(registry2.listenerCount()).thenReturn(3);

        int count = compositeRegistry.listenerCount();

        assertEquals(8, count);
    }

    @Test
    void testRegister_DelegatesToAllRegistries() {
        Object listener = new Object();

        compositeRegistry.register(listener);

        verify(registry1).register(listener);
        verify(registry2).register(listener);
    }

    @Test
    void testUnregister_AnyRegistryReturnsTrue() {
        Object listener = new Object();
        when(registry1.unregister(listener)).thenReturn(true);
        when(registry2.unregister(listener)).thenReturn(false);

        boolean result = compositeRegistry.unregister(listener);

        assertTrue(result);
        verify(registry1).unregister(listener);
        verify(registry2).unregister(listener);
    }

    @Test
    void testUnregister_AllRegistriesReturnFalse() {
        Object listener = new Object();
        when(registry1.unregister(listener)).thenReturn(false);
        when(registry2.unregister(listener)).thenReturn(false);

        boolean result = compositeRegistry.unregister(listener);

        assertFalse(result);
    }

    @Test
    void testIsRegistered_AnyRegistryReturnsTrue() {
        Object listener = new Object();
        when(registry1.isRegistered(listener)).thenReturn(false);
        when(registry2.isRegistered(listener)).thenReturn(true);

        boolean result = compositeRegistry.isRegistered(listener);

        assertTrue(result);
    }

    @Test
    void testIsRegistered_AllRegistriesReturnFalse() {
        Object listener = new Object();
        when(registry1.isRegistered(listener)).thenReturn(false);
        when(registry2.isRegistered(listener)).thenReturn(false);

        boolean result = compositeRegistry.isRegistered(listener);

        assertFalse(result);
    }

    @Test
    void testMerge_DelegatesToAllRegistries() {
        EventListenerRegistry otherRegistry = mock(EventListenerRegistry.class);
        doThrow(new UnsupportedOperationException("Merge not supported")).when(registry1).merge(otherRegistry);

        assertThrows(UnsupportedOperationException.class, () -> compositeRegistry.merge(otherRegistry));
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
}
