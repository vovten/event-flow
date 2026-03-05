package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CompositeEventListenerRegistry
 */
class CompositeEventListenerRegistryTest {

    private CompositeEventListenerRegistry compositeRegistry;

    @BeforeEach
    void setUp() {
    }

    @Test
    @DisplayName("Should compose multiple listener registries")
    void shouldComposeMultipleListenerRegistries() {
        InterfaceEventListenerRegistry registry1 = new InterfaceEventListenerRegistry();
        AnnotationEventListenerRegistry registry2 = new AnnotationEventListenerRegistry();
        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        assertEquals(0, compositeRegistry.listenerCount());
    }

    @Test
    @DisplayName("Should get listeners from all composed registries")
    void shouldGetListenersFromAllComposedRegistries() {
        InterfaceEventListenerRegistry interfaceRegistry = new InterfaceEventListenerRegistry();
        TestEventListener listener = new TestEventListener();
        interfaceRegistry.register(listener);

        AnnotationEventListenerRegistry annotationRegistry = new AnnotationEventListenerRegistry();
        AnnotatedEventListener annotatedListener = new AnnotatedEventListener();
        annotationRegistry.register(annotatedListener);

        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(interfaceRegistry, annotationRegistry)));

        TestEvent event = TestEvent.create("Test message");

        var listeners = compositeRegistry.getListeners(event);
        assertEquals(2, listeners.size());
    }

    @Test
    @DisplayName("Should register listener to all composed registries")
    void shouldRegisterListenerToAllComposedRegistries() {
        InterfaceEventListenerRegistry registry1 = new InterfaceEventListenerRegistry();
        InterfaceEventListenerRegistry registry2 = new InterfaceEventListenerRegistry();
        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        TestEventListener listener = new TestEventListener();
        compositeRegistry.register(listener);

        assertTrue(registry1.isRegistered(listener));
        assertTrue(registry2.isRegistered(listener));
    }

    @Test
    @DisplayName("Should calculate total listener count from all registries")
    void shouldCalculateTotalListenerCountFromAllRegistries() {
        InterfaceEventListenerRegistry registry1 = new InterfaceEventListenerRegistry();
        InterfaceEventListenerRegistry registry2 = new InterfaceEventListenerRegistry();

        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();
        registry1.register(listener1);
        registry2.register(listener2);

        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        assertEquals(2, compositeRegistry.listenerCount());
    }

    @Test
    @DisplayName("Should unregister listener from all registries")
    void shouldUnregisterListenerFromAllRegistries() {
        InterfaceEventListenerRegistry registry1 = new InterfaceEventListenerRegistry();
        InterfaceEventListenerRegistry registry2 = new InterfaceEventListenerRegistry();
        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        TestEventListener listener = new TestEventListener();
        compositeRegistry.register(listener);

        boolean result = compositeRegistry.unregister(listener);
        assertTrue(result);
        assertFalse(registry1.isRegistered(listener));
        assertFalse(registry2.isRegistered(listener));
    }

    @Test
    @DisplayName("Should support merging listener registry")
    void shouldSupportMergingListenerRegistry() {
        InterfaceEventListenerRegistry registry1 = new InterfaceEventListenerRegistry();
        TestEventListener listener = new TestEventListener();
        registry1.register(listener);

        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1))
        );

        InterfaceEventListenerRegistry registry2 = new InterfaceEventListenerRegistry();
        TestEventListener listener2 = new TestEventListener();
        registry2.register(listener2);

        compositeRegistry.merge(registry2);

        assertEquals(2, compositeRegistry.listenerCount());
    }

    // Test helper class
    static class TestEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    // Test helper class with @EventListener annotation
    static class AnnotatedEventListener {
        @com.github.vovten.eventflow.annotation.EventListener
        public void handleTestEvent(TestEvent event) {
        }
    }
}
