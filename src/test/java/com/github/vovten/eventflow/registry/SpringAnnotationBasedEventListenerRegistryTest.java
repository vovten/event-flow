package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SpringAnnotationBasedEventListenerRegistry
 */
class SpringAnnotationBasedEventListenerRegistryTest {

    @Test
    @DisplayName("Should register listener with @EventListener annotation")
    void shouldRegisterListenerWithEventListenerAnnotation() {
        SpringAnnotationBasedEventListenerRegistry registry = new SpringAnnotationBasedEventListenerRegistry();
        AnnotatedEventListener listener = new AnnotatedEventListener();
        registry.register(listener);
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should return listeners for annotated listener method")
    void shouldReturnListenersForAnnotatedListenerMethod() {
        SpringAnnotationBasedEventListenerRegistry registry = new SpringAnnotationBasedEventListenerRegistry();
        AnnotatedEventListener listener = new AnnotatedEventListener();
        registry.register(listener);
        TestEvent event = TestEvent.create("Test message");
        var listeners = registry.getListeners(event);
        assertEquals(1, listeners.size());
    }

    @Test
    @DisplayName("Should initialize listeners from application context")
    void shouldInitializeListenersFromApplicationContext() {
        AnnotatedEventListener listener = new AnnotatedEventListener();
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBeanDefinitionNames())
            .thenReturn(new String[]{"annotatedEventListener"});
        when(applicationContext.getBean("annotatedEventListener")).thenReturn(listener);

        SpringAnnotationBasedEventListenerRegistry registry = new SpringAnnotationBasedEventListenerRegistry(
            "", applicationContext);

        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should throw exception for invalid method signature")
    void shouldThrowExceptionForInvalidMethodSignature() {
        SpringAnnotationBasedEventListenerRegistry registry = new SpringAnnotationBasedEventListenerRegistry();
        InvalidEventListener listener = new InvalidEventListener();
        InvalidEventListenerMethodSignatureException exception = assertThrows(
            InvalidEventListenerMethodSignatureException.class,
            () -> registry.register(listener)
        );
        assertTrue(exception.getMessage().contains("Method signature"));
    }

    @Test
    @DisplayName("Should return empty list when no listeners registered")
    void shouldReturnEmptyListWhenNoListenersRegistered() {
        SpringAnnotationBasedEventListenerRegistry registry = new SpringAnnotationBasedEventListenerRegistry();
        TestEvent event = TestEvent.create();
        var listeners = registry.getListeners(event);
        assertTrue(listeners.isEmpty());
    }

    @Test
    @DisplayName("Should not support merging registries")
    void shouldNotSupportMergingRegistries() {
        SpringAnnotationBasedEventListenerRegistry registry = new SpringAnnotationBasedEventListenerRegistry();
        EventListenerRegistry otherRegistry = mock(EventListenerRegistry.class);
        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    @DisplayName("Should return true for isEmpty when no listeners")
    void shouldReturnTrueForIsEmptyWhenNoListeners() {
        SpringAnnotationBasedEventListenerRegistry registry = new SpringAnnotationBasedEventListenerRegistry();
        assertEquals(0, registry.listenerCount());
    }

    @Test
    @DisplayName("Should unregister listener")
    void shouldUnregisterListener() {
        SpringAnnotationBasedEventListenerRegistry registry = new SpringAnnotationBasedEventListenerRegistry();
        AnnotatedEventListener listener = new AnnotatedEventListener();
        registry.register(listener);
        assertTrue(registry.isRegistered(listener));
        
        boolean result = registry.unregister(listener);
        assertTrue(result);
        assertFalse(registry.isRegistered(listener));
    }

    // Test helper class with @EventListener annotation
    static class AnnotatedEventListener {
        @com.github.vovten.eventflow.annotation.EventListener
        public void handleTestEvent(TestEvent event) {
        }
    }

    // Test helper class with invalid method signature
    static class InvalidEventListener {
        @com.github.vovten.eventflow.annotation.EventListener
        public void handleEvent(String invalidParam) {
        }
    }

    // Test helper class that handles generic Event
    static class GenericEventListener {
        @com.github.vovten.eventflow.annotation.EventListener
        public void handleEvent(Event event) {
        }
    }
}
