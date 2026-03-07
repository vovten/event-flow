package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for CompositeEventListenerRegistry.
 */
@DisplayName("CompositeEventListenerRegistry Tests")
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
    @DisplayName("Should throw exception for empty registries list")
    void shouldThrowExceptionForEmptyRegistriesList() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeEventListenerRegistry(List.of()));
    }

    @Test
    @DisplayName("Should throw exception for null registries list")
    void shouldThrowExceptionForNullRegistriesList() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeEventListenerRegistry(null));
    }

    @Test
    @DisplayName("Should create registry with registries")
    void shouldCreateRegistryWithRegistries() {
        assertDoesNotThrow(() -> new CompositeEventListenerRegistry(List.of(registry1)));
    }

    @Test
    @DisplayName("Should combine listeners from all registries")
    void shouldCombineListenersFromAllRegistries() {
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
    @DisplayName("Should return empty list when all registries are empty")
    void shouldReturnEmptyListWhenAllRegistriesAreEmpty() {
        when(registry1.getListeners(any())).thenReturn(List.of());
        when(registry2.getListeners(any())).thenReturn(List.of());

        TestEvent event = new TestEvent();
        List<EventListener> listeners = compositeRegistry.getListeners(event);

        assertTrue(listeners.isEmpty());
    }

    @Test
    @DisplayName("Should sum listener counts from all registries")
    void shouldSumListenerCountsFromAllRegistries() {
        when(registry1.listenerCount()).thenReturn(5);
        when(registry2.listenerCount()).thenReturn(3);

        int count = compositeRegistry.listenerCount();

        assertEquals(8, count);
    }

    @Test
    @DisplayName("Should delegate register to all registries")
    void shouldDelegateRegisterToAllRegistries() {
        Object listener = new Object();

        compositeRegistry.register(listener);

        verify(registry1).register(listener);
        verify(registry2).register(listener);
    }

    @Test
    @DisplayName("Should return true when any registry returns true on unregister")
    void shouldReturnTrueWhenAnyRegistryReturnsTrueOnUnregister() {
        Object listener = new Object();
        when(registry1.unregister(listener)).thenReturn(true);
        when(registry2.unregister(listener)).thenReturn(false);

        boolean result = compositeRegistry.unregister(listener);

        assertTrue(result);
        verify(registry1).unregister(listener);
        verify(registry2).unregister(listener);
    }

    @Test
    @DisplayName("Should return false when all registries return false on unregister")
    void shouldReturnFalseWhenAllRegistriesReturnFalseOnUnregister() {
        Object listener = new Object();
        when(registry1.unregister(listener)).thenReturn(false);
        when(registry2.unregister(listener)).thenReturn(false);

        boolean result = compositeRegistry.unregister(listener);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should return true when any registry returns true on isRegistered")
    void shouldReturnTrueWhenAnyRegistryReturnsTrueOnIsRegistered() {
        Object listener = new Object();
        when(registry1.isRegistered(listener)).thenReturn(false);
        when(registry2.isRegistered(listener)).thenReturn(true);

        boolean result = compositeRegistry.isRegistered(listener);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should return false when all registries return false on isRegistered")
    void shouldReturnFalseWhenAllRegistriesReturnFalseOnIsRegistered() {
        Object listener = new Object();
        when(registry1.isRegistered(listener)).thenReturn(false);
        when(registry2.isRegistered(listener)).thenReturn(false);

        boolean result = compositeRegistry.isRegistered(listener);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should throw exception on merge")
    void shouldThrowExceptionOnMerge() {
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
