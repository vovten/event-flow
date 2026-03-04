package com.github.vovten.eventflow.collection;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import com.github.vovten.eventflow.annotation.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SpringAnnotatedEventListenerRegistry
 */
class SpringAnnotatedEventListenerRegistryTest {

    private ExecutorService executorService;
    private ApplicationContext applicationContext;
    private SpringAnnotatedEventListenerRegistry registry;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        applicationContext = mock(ApplicationContext.class);
    }

    @Test
    @DisplayName("Should register listener with @EventListener annotation")
    void shouldRegisterListenerWithEventListenerAnnotation() {
        // given
        registry = new SpringAnnotatedEventListenerRegistry(executorService);
        AnnotatedEventListener listener = new AnnotatedEventListener();

        // when
        registry.register(listener);

        // then
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should dispatch event to registered listener method")
    void shouldDispatchEventToRegisteredListenerMethod() throws InterruptedException {
        // given
        registry = new SpringAnnotatedEventListenerRegistry(executorService);
        AnnotatedEventListener listener = new AnnotatedEventListener();
        registry.register(listener);
        TestEvent event = TestEvent.create("Test message");

        // when
        boolean result = registry.dispatch(event);

        // then
        Thread.sleep(100);
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertEquals("Test message", listener.getLastEvent().getMessage());
    }

    @Test
    @DisplayName("Should initialize listeners from application context")
    void shouldInitializeListenersFromApplicationContext() {
        // given
        AnnotatedEventListener listener = new AnnotatedEventListener();
        when(applicationContext.getBeanDefinitionNames())
            .thenReturn(new String[]{"annotatedEventListener"});
        when(applicationContext.getBean("annotatedEventListener")).thenReturn(listener);

        // when
        registry = new SpringAnnotatedEventListenerRegistry(
            "", executorService, applicationContext);

        // then
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should filter beans by scan package")
    void shouldFilterBeansByScanPackage() {
        // given
        AnnotatedEventListener listener = new AnnotatedEventListener();
        when(applicationContext.getBeanDefinitionNames())
            .thenReturn(new String[]{"annotatedEventListener"});
        when(applicationContext.getBean("annotatedEventListener")).thenReturn(listener);

        // when
        registry = new SpringAnnotatedEventListenerRegistry(
            "com.github.vovten", executorService, applicationContext);

        // then
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should throw exception for invalid method signature")
    void shouldThrowExceptionForInvalidMethodSignature() {
        // given
        registry = new SpringAnnotatedEventListenerRegistry(executorService);
        InvalidEventListener listener = new InvalidEventListener();

        // when & then
        InvalidEventListenerMethodSignatureException exception = assertThrows(
            InvalidEventListenerMethodSignatureException.class,
            () -> registry.register(listener)
        );
        assertTrue(exception.getMessage().contains("Method signature"));
    }

    @Test
    @DisplayName("Should return false when no listeners registered")
    void shouldReturnFalseWhenNoListenersRegistered() {
        // given
        registry = new SpringAnnotatedEventListenerRegistry(executorService);
        TestEvent event = TestEvent.create();

        // when
        boolean result = registry.dispatch(event);

        // then
        assertFalse(result);
    }

    @Test
    @DisplayName("Should handle Event.class as parameter type")
    void shouldHandleEventClassAsParameterType() throws InterruptedException {
        // given
        registry = new SpringAnnotatedEventListenerRegistry(executorService);
        GenericEventListener listener = new GenericEventListener();
        registry.register(listener);
        TestEvent event = TestEvent.create();

        // when
        boolean result = registry.dispatch(event);

        // then
        Thread.sleep(100);
        assertTrue(result);
        assertTrue(listener.wasCalled());
    }

    @Test
    @DisplayName("Should not support merging registries")
    void shouldNotSupportMergingRegistries() {
        // given
        registry = new SpringAnnotatedEventListenerRegistry(executorService);
        EventListenerRegistry otherRegistry = mock(EventListenerRegistry.class);

        // when & then
        assertThrows(
            UnsupportedOperationException.class,
            () -> registry.merge(otherRegistry)
        );
    }

    @Test
    @DisplayName("Should return correct listener count")
    void shouldReturnCorrectListenerCount() {
        // given
        registry = new SpringAnnotatedEventListenerRegistry(executorService);
        AnnotatedEventListener listener1 = new AnnotatedEventListener();
        AnnotatedEventListener listener2 = new AnnotatedEventListener();

        // when
        registry.register(listener1);
        registry.register(listener2);

        // then
        assertEquals(1, registry.listenerCount());
    }

    @Test
    @DisplayName("Should return true for hasListeners when no listeners")
    void shouldReturnTrueForHasListenersWhenNoListeners() {
        // given
        registry = new SpringAnnotatedEventListenerRegistry(executorService);

        // then
        assertFalse(registry.hasListeners());
    }

    // Test helper class with @EventListener annotation
    static class AnnotatedEventListener {
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

    // Test helper class with invalid method signature
    static class InvalidEventListener {
        @EventListener
        public void handleEvent(String invalidParam) {
            // Invalid - should accept Event, not String
        }
    }

    // Test helper class that handles generic Event
    static class GenericEventListener {
        private boolean called = false;

        @EventListener
        public void handleEvent(Event event) {
            this.called = true;
        }

        boolean wasCalled() {
            return called;
        }
    }
}
