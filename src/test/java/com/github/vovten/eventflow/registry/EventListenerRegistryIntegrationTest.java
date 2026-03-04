package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventFlowTestApplication;
import com.github.vovten.eventflow.annotation.EventListener;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for EventListenerRegistry implementations
 */
@SpringBootTest(classes = EventFlowTestApplication.class)
class EventListenerRegistryIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    @DisplayName("Should dispatch event to interface-based listener")
    void shouldDispatchEventToInterfaceBasedListener() throws InterruptedException {
        // given
        SpringInterfaceBasedEventListenerRegistry registry =
            new SpringInterfaceBasedEventListenerRegistry(
                Executors.newFixedThreadPool(2));
        InterfaceBasedListener listener = new InterfaceBasedListener();
        registry.register(listener);
        TestEvent event = TestEvent.create("Interface listener test");

        // when
        boolean result = registry.dispatch(event);
        Thread.sleep(100);

        // then
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertNotNull(listener.getLastEvent());
        assertEquals("Interface listener test", listener.getLastEvent().getMessage());
    }

    @Test
    @DisplayName("Should dispatch event to annotation-based listener")
    void shouldDispatchEventToAnnotationBasedListener() throws InterruptedException {
        // given
        SpringAnnotatedEventListenerRegistry registry =
            new SpringAnnotatedEventListenerRegistry(
                Executors.newFixedThreadPool(2));
        AnnotationBasedListener listener = new AnnotationBasedListener();
        registry.register(listener);
        TestEvent event = TestEvent.create("Annotation listener test");

        // when
        boolean result = registry.dispatch(event);
        Thread.sleep(100);

        // then
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertNotNull(listener.getLastEvent());
        assertEquals("Annotation listener test", listener.getLastEvent().getMessage());
    }

    @Test
    @DisplayName("Should dispatch event through composite registry")
    void shouldDispatchEventThroughCompositeRegistry() throws InterruptedException {
        // given
        SpringInterfaceBasedEventListenerRegistry interfaceRegistry =
            new SpringInterfaceBasedEventListenerRegistry(
                Executors.newFixedThreadPool(2));
        SpringAnnotatedEventListenerRegistry annotationRegistry =
            new SpringAnnotatedEventListenerRegistry(
                Executors.newFixedThreadPool(2));

        InterfaceBasedListener interfaceListener = new InterfaceBasedListener();
        AnnotationBasedListener annotationListener = new AnnotationBasedListener();

        interfaceRegistry.register(interfaceListener);
        annotationRegistry.register(annotationListener);

        CompositeEventListenerRegistry compositeRegistry =
            new CompositeEventListenerRegistry(
                new java.util.ArrayList<>(List.of(interfaceRegistry, annotationRegistry)));

        TestEvent event = TestEvent.create("Composite listener test");

        // when
        boolean result = compositeRegistry.dispatch(event);
        Thread.sleep(100);

        // then
        assertTrue(result);
        assertTrue(interfaceListener.wasCalled());
        assertTrue(annotationListener.wasCalled());
    }

    @Test
    @DisplayName("Should initialize listeners from Spring context")
    void shouldInitializeListenersFromSpringContext() throws InterruptedException {
        // given
        SpringInterfaceBasedEventListenerRegistry registry =
            new SpringInterfaceBasedEventListenerRegistry(
                Executors.newFixedThreadPool(2),
                applicationContext);

        SpringAnnotatedEventListenerRegistry annotationRegistry =
            new SpringAnnotatedEventListenerRegistry(
                "",
                Executors.newFixedThreadPool(2),
                applicationContext);

        TestEvent event = TestEvent.create("Spring context test");

        // when
        boolean result1 = registry.dispatch(event);
        boolean result2 = annotationRegistry.dispatch(event);
        Thread.sleep(100);

        // then
        // Registries should be initialized with beans from context
        assertTrue(result1 || result2);
    }

    // Interface-based listener
    public static class InterfaceBasedListener implements com.github.vovten.eventflow.EventListener {
        private boolean called = false;
        private TestEvent lastEvent;

        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            this.called = true;
            this.lastEvent = (TestEvent) event;
        }

        boolean wasCalled() {
            return called;
        }

        TestEvent getLastEvent() {
            return lastEvent;
        }
    }

    // Annotation-based listener
    @Component
    public static class AnnotationBasedListener {
        private boolean called = false;
        private TestEvent lastEvent;

        @EventListener
        public void handleTestEvent(TestEvent event) {
            this.called = true;
            this.lastEvent = event;
        }

        boolean wasCalled() {
            return called;
        }

        TestEvent getLastEvent() {
            return lastEvent;
        }
    }
}
