package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.annotation.EventListener;
import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CompositeEventListenerRegistry
 */
class CompositeEventListenerRegistryTest {

    private ExecutorService executorService;
    private CompositeEventListenerRegistry compositeRegistry;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
    }

    @Test
    @DisplayName("Should compose multiple listener registries")
    void shouldComposeMultipleListenerRegistries() {
        // given
        SpringInterfaceBasedEventListenerRegistry registry1 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        SpringAnnotatedEventListenerRegistry registry2 =
                new SpringAnnotatedEventListenerRegistry(executorService);
        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        // when & then
        assertFalse(compositeRegistry.hasListeners());
        assertEquals(0, compositeRegistry.listenerCount());
    }

    @Test
    @DisplayName("Should dispatch event to all composed registries")
    void shouldDispatchEventToAllComposedRegistries() throws InterruptedException {
        // given
        SpringInterfaceBasedEventListenerRegistry interfaceRegistry =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        TestEventListener listener = new TestEventListener();
        interfaceRegistry.register(listener);

        SpringAnnotatedEventListenerRegistry annotationRegistry =
                new SpringAnnotatedEventListenerRegistry(executorService);
        AnnotatedEventListener annotatedListener = new AnnotatedEventListener();
        annotationRegistry.register(annotatedListener);

        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(interfaceRegistry, annotationRegistry)));

        TestEvent event = TestEvent.create("Test message");

        // when
        boolean result = compositeRegistry.dispatch(event);

        // then
        Thread.sleep(100);
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertTrue(annotatedListener.wasCalled());
    }

    @Test
    @DisplayName("Should return false when no registries have listeners for event")
    void shouldReturnFalseWhenNoRegistriesHaveListenersForEvent() {
        // given
        SpringInterfaceBasedEventListenerRegistry registry1 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        SpringAnnotatedEventListenerRegistry registry2 =
                new SpringAnnotatedEventListenerRegistry(executorService);
        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        TestEvent event = TestEvent.create();

        // when
        boolean result = compositeRegistry.dispatch(event);

        // then
        assertFalse(result);
    }

    @Test
    @DisplayName("Should register listener to all composed registries")
    void shouldRegisterListenerToAllComposedRegistries() {
        // given
        SpringInterfaceBasedEventListenerRegistry registry1 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        SpringInterfaceBasedEventListenerRegistry registry2 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        TestEventListener listener = new TestEventListener();

        // when
        compositeRegistry.register(listener);

        // then
        assertTrue(registry1.isRegistered(listener));
        assertTrue(registry2.isRegistered(listener));
    }

    @Test
    @DisplayName("Should check isRegistered in all composed registries")
    void shouldCheckIsRegisteredInAllComposedRegistries() {
        // given
        SpringInterfaceBasedEventListenerRegistry registry1 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        SpringInterfaceBasedEventListenerRegistry registry2 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        TestEventListener listener = new TestEventListener();
        registry1.register(listener);

        // when & then
        assertTrue(compositeRegistry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should calculate total listener count from all registries")
    void shouldCalculateTotalListenerCountFromAllRegistries() {
        // given
        SpringInterfaceBasedEventListenerRegistry registry1 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        SpringInterfaceBasedEventListenerRegistry registry2 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);

        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();
        registry1.register(listener1);
        registry2.register(listener2);

        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        // when & then
        assertEquals(2, compositeRegistry.listenerCount());
    }

    @Test
    @DisplayName("Should return false for hasListeners when any registry has listeners")
    void shouldReturnFalseForHasListenersWhenAnyRegistryHasListeners() {
        // given
        SpringInterfaceBasedEventListenerRegistry registry1 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        SpringInterfaceBasedEventListenerRegistry registry2 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);

        TestEventListener listener = new TestEventListener();
        registry1.register(listener);

        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1, registry2)));

        // when & then
        assertTrue(compositeRegistry.hasListeners());
    }

    @Test
    @DisplayName("Should support merging listener registry")
    void shouldSupportMergingListenerRegistry() {
        // given
        SpringInterfaceBasedEventListenerRegistry registry1 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        TestEventListener listener = new TestEventListener();
        registry1.register(listener);

        compositeRegistry = new CompositeEventListenerRegistry(
                new ArrayList<>(List.of(registry1))
        );

        SpringInterfaceBasedEventListenerRegistry registry2 =
                new SpringInterfaceBasedEventListenerRegistry(executorService);
        TestEventListener listener2 = new TestEventListener();
        registry2.register(listener2);

        // when
        compositeRegistry.merge(registry2);

        // then
        assertEquals(2, compositeRegistry.listenerCount());
    }

    // Test helper class
    static class TestEventListener implements com.github.vovten.eventflow.EventListener {
        private boolean called = false;

        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            this.called = true;
        }

        boolean wasCalled() {
            return called;
        }
    }

    // Test helper class with @EventListener annotation
    static class AnnotatedEventListener {
        private boolean called = false;

        @EventListener
        public void handleTestEvent(TestEvent event) {
            this.called = true;
        }

        boolean wasCalled() {
            return called;
        }
    }
}
