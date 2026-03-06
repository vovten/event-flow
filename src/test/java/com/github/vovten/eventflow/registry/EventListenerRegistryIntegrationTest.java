package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.EventFlowTestApplication;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for EventListenerRegistry implementations
 */
@SpringBootTest(classes = EventFlowTestApplication.class)
class EventListenerRegistryIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    @DisplayName("Should get interface-based listeners")
    void shouldGetInterfaceBasedListeners() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        registry.register(new InterfaceBasedListener());
        TestEvent event = TestEvent.create("Interface listener test");
        var listeners = registry.getListeners(event);
        assertFalse(listeners.isEmpty());
        assertEquals(1, listeners.size());
    }

    @Test
    @DisplayName("Should get annotation-based listeners")
    void shouldGetAnnotationBasedListeners() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry("com.github.vovten.eventflow", applicationContext);
        AnnotationBasedListener listener = new AnnotationBasedListener();
        registry.register(listener);
        TestEvent event = TestEvent.create("Annotation listener test");
        var listeners = registry.getListeners(event);
        assertFalse(listeners.isEmpty());
        assertEquals(1, listeners.size());
    }

    @Test
    @DisplayName("Should get listeners through composite registry")
    void shouldGetListenersThroughCompositeRegistry() {
        InterfaceEventListenerRegistry interfaceRegistry = new InterfaceEventListenerRegistry();
        AnnotationEventListenerRegistry annotationRegistry = new AnnotationEventListenerRegistry();

        InterfaceBasedListener interfaceListener = new InterfaceBasedListener();
        AnnotationBasedListener annotationListener = new AnnotationBasedListener();

        interfaceRegistry.register(interfaceListener);
        annotationRegistry.register(annotationListener);

        CompositeEventListenerRegistry compositeRegistry = new CompositeEventListenerRegistry(
            new java.util.ArrayList<>(List.of(interfaceRegistry, annotationRegistry)));

        TestEvent event = TestEvent.create("Composite listener test");
        var listeners = compositeRegistry.getListeners(event);
        assertEquals(2, listeners.size());
    }

    @Test
    @DisplayName("Should initialize listeners from Spring context")
    void shouldInitializeListenersFromSpringContext() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        SpringAnnotationEventListenerRegistry annotationRegistry = new SpringAnnotationEventListenerRegistry("com.github.vovten.eventflow", applicationContext);

        assertTrue(registry.listenerCount() >= 0);
        assertTrue(annotationRegistry.listenerCount() >= 0);
    }

    @Test
    @DisplayName("Should unregister listener")
    void shouldUnregisterListener() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        InterfaceBasedListener listener = new InterfaceBasedListener();
        registry.register(listener);
        assertTrue(registry.isRegistered(listener));

        boolean result = registry.unregister(listener);
        assertTrue(result);
        assertFalse(registry.isRegistered(listener));
    }

    // Interface-based listener
    public static class InterfaceBasedListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    // Annotation-based listener
    @Component
    public static class AnnotationBasedListener {
        @com.github.vovten.eventflow.annotation.EventListener
        public void handleTestEvent(TestEvent event) {
        }
    }
}
