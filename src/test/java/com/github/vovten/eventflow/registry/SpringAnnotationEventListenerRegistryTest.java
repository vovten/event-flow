package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SpringAnnotationEventListenerRegistry
 */
class SpringAnnotationEventListenerRegistryTest {

    private final ApplicationContext applicationContext = mock(ApplicationContext.class);
    private static final String SCAN_PACKAGE = "com.github.vovten.eventflow";

    SpringAnnotationEventListenerRegistryTest() {
        when(applicationContext.getBeanDefinitionNames()).thenReturn(new String[]{});
    }

    @Test
    @DisplayName("Should register listener with @EventListener annotation")
    void shouldRegisterListenerWithEventListenerAnnotation() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, SCAN_PACKAGE);
        AnnotatedEventListener listener = new AnnotatedEventListener();
        registry.register(listener);
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should return listeners for annotated listener method")
    void shouldReturnListenersForAnnotatedListenerMethod() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, SCAN_PACKAGE);
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
        when(applicationContext.getBeanDefinitionNames())
            .thenReturn(new String[]{"annotatedEventListener"});
        when(applicationContext.getBean("annotatedEventListener")).thenReturn(listener);

        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(
                applicationContext, SCAN_PACKAGE);

        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should throw exception for invalid method signature")
    void shouldThrowExceptionForInvalidMethodSignature() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, SCAN_PACKAGE);
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
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, SCAN_PACKAGE);
        TestEvent event = TestEvent.create();
        var listeners = registry.getListeners(event);
        assertTrue(listeners.isEmpty());
    }

    @Test
    @DisplayName("Should not support merging registries")
    void shouldNotSupportMergingRegistries() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, SCAN_PACKAGE);
        EventListenerRegistry otherRegistry = mock(EventListenerRegistry.class);
        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    @DisplayName("Should return true for isEmpty when no listeners")
    void shouldReturnTrueForIsEmptyWhenNoListeners() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, SCAN_PACKAGE);
        assertEquals(0, registry.listenerCount());
    }

    @Test
    @DisplayName("Should unregister listener")
    void shouldUnregisterListener() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, SCAN_PACKAGE);
        AnnotatedEventListener listener = new AnnotatedEventListener();
        registry.register(listener);
        assertTrue(registry.isRegistered(listener));

        boolean result = registry.unregister(listener);
        assertTrue(result);
        assertFalse(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when applicationContext is null")
    void shouldThrowIllegalArgumentExceptionWhenApplicationContextIsNull() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new SpringAnnotationEventListenerRegistry(null, SCAN_PACKAGE)
        );
        assertEquals("ApplicationContext is required", exception.getMessage());
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
