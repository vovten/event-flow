package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SpringAnnotationEventListenerRegistry.
 */
@DisplayName("SpringAnnotationEventListenerRegistry Tests")
class SpringAnnotationEventListenerRegistryTest {

    private ApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBeanDefinitionNames()).thenReturn(new String[]{});
    }

    @Test
    @DisplayName("Should throw exception for null context")
    void shouldThrowExceptionForNullContext() {
        assertThrows(IllegalArgumentException.class, () ->
                new SpringAnnotationEventListenerRegistry(null, "com.example"));
    }

    @Test
    @DisplayName("Should throw exception for null scan package")
    void shouldThrowExceptionForNullScanPackage() {
        assertThrows(IllegalStateException.class, () ->
                new SpringAnnotationEventListenerRegistry(applicationContext, null));
    }

    @Test
    @DisplayName("Should throw exception for empty scan package")
    void shouldThrowExceptionForEmptyScanPackage() {
        assertThrows(IllegalStateException.class, () ->
                new SpringAnnotationEventListenerRegistry(applicationContext, ""));
    }

    @Test
    @DisplayName("Should throw exception for invalid scan package")
    void shouldThrowExceptionForInvalidScanPackage() {
        assertThrows(IllegalArgumentException.class, () ->
                new SpringAnnotationEventListenerRegistry(applicationContext, "invalid-package"));
    }

    @Test
    @DisplayName("Should create registry with valid parameters")
    void shouldCreateRegistryWithValidParameters() {
        assertDoesNotThrow(() ->
                new SpringAnnotationEventListenerRegistry(applicationContext, "com.example"));
    }

    @Test
    @DisplayName("Should register annotated method")
    void shouldRegisterAnnotatedMethod() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, "com.example");
        TestAnnotatedListener listener = new TestAnnotatedListener();

        registry.register(listener);

        assertEquals(1, registry.listenerCount());
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should register multiple annotated methods")
    void shouldRegisterMultipleAnnotatedMethods() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, "com.example");
        MultiMethodListener listener = new MultiMethodListener();

        registry.register(listener);

        assertEquals(2, registry.listenerCount());
    }

    @Test
    @DisplayName("Should ignore methods without annotation")
    void shouldIgnoreMethodsWithoutAnnotation() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, "com.example");
        NoAnnotationListener listener = new NoAnnotationListener();

        registry.register(listener);

        assertEquals(0, registry.listenerCount());
    }

    @Test
    @DisplayName("Should throw exception for invalid method signature")
    void shouldThrowExceptionForInvalidMethodSignature() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, "com.example");
        InvalidSignatureListener listener = new InvalidSignatureListener();

        assertThrows(InvalidEventListenerMethodSignatureException.class, () -> registry.register(listener));
    }

    @Test
    @DisplayName("Should return listeners for event type")
    void shouldReturnListenersForEventType() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, "com.example");
        TestAnnotatedListener listener = new TestAnnotatedListener();
        registry.register(listener);

        TestEvent event = new TestEvent();
        List<com.github.vovten.eventflow.EventListener> listeners = registry.getListeners(event);

        assertEquals(1, listeners.size());
    }

    @Test
    @DisplayName("Should unregister existing listener")
    void shouldUnregisterExistingListener() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, "com.example");
        TestAnnotatedListener listener = new TestAnnotatedListener();
        registry.register(listener);

        boolean result = registry.unregister(listener);

        assertTrue(result);
        assertFalse(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should throw exception on merge")
    void shouldThrowExceptionOnMerge() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, "com.example");
        SpringAnnotationEventListenerRegistry otherRegistry = mock(SpringAnnotationEventListenerRegistry.class);

        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    @DisplayName("Should invoke listener method")
    void shouldInvokeListenerMethod() {
        SpringAnnotationEventListenerRegistry registry = new SpringAnnotationEventListenerRegistry(applicationContext, "com.example");
        TestAnnotatedListener listener = new TestAnnotatedListener();
        registry.register(listener);

        TestEvent event = new TestEvent();
        List<com.github.vovten.eventflow.EventListener> listeners = registry.getListeners(event);

        assertEquals(1, listeners.size());
        listeners.get(0).onEvent(event);

        assertTrue(listener.wasCalled());
    }

    static class TestAnnotatedListener {
        private boolean called = false;

        @com.github.vovten.eventflow.annotation.EventListener
        public void handleTestEvent(TestEvent event) {
            this.called = true;
        }

        boolean wasCalled() {
            return called;
        }
    }

    static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    static class MultiMethodListener {
        @com.github.vovten.eventflow.annotation.EventListener
        public void handleTestEvent(TestEvent event) {
        }

        @com.github.vovten.eventflow.annotation.EventListener
        public void handleSpecificEvent(SpecificEvent event) {
        }
    }

    static class NoAnnotationListener {
        public void handleEvent(TestEvent event) {
        }
    }

    static class InvalidSignatureListener {
        @com.github.vovten.eventflow.annotation.EventListener
        public void handleEvent(String invalidParam) {
        }
    }

    static class SpecificEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return SpecificEvent.class;
        }
    }
}
