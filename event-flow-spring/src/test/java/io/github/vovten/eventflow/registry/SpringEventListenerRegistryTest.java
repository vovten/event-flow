package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SpringEventListenerRegistry.
 */
@DisplayName("SpringEventListenerRegistry Tests")
class SpringEventListenerRegistryTest {

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
                new SpringEventListenerRegistry(null, "com.example"));
    }

    @Test
    @DisplayName("Should throw exception for null scan package")
    void shouldThrowExceptionForNullScanPackage() {
        assertThrows(IllegalStateException.class, () ->
                new SpringEventListenerRegistry(applicationContext, null));
    }

    @Test
    @DisplayName("Should throw exception for empty scan package")
    void shouldThrowExceptionForEmptyScanPackage() {
        assertThrows(IllegalStateException.class, () ->
                new SpringEventListenerRegistry(applicationContext, ""));
    }

    @Test
    @DisplayName("Should throw exception for invalid scan package")
    void shouldThrowExceptionForInvalidScanPackage() {
        assertThrows(IllegalArgumentException.class, () ->
                new SpringEventListenerRegistry(applicationContext, "invalid-package"));
    }

    @Test
    @DisplayName("Should create registry with valid parameters")
    void shouldCreateRegistryWithValidParameters() {
        assertDoesNotThrow(() ->
                new SpringEventListenerRegistry(applicationContext, "com.example"));
    }

    @Test
    @DisplayName("Should register annotated method")
    void shouldRegisterAnnotatedMethod() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        TestAnnotatedListener listener = new TestAnnotatedListener();

        registry.register(listener);

        assertEquals(1, registry.handlerCount());
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should register multiple annotated methods")
    void shouldRegisterMultipleAnnotatedMethods() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        MultiMethodListener listener = new MultiMethodListener();

        registry.register(listener);

        assertEquals(2, registry.handlerCount());
    }

    @Test
    @DisplayName("Should ignore methods without annotation")
    void shouldIgnoreMethodsWithoutAnnotation() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        NoAnnotationListener listener = new NoAnnotationListener();

        registry.register(listener);

        assertEquals(0, registry.handlerCount());
    }

    @Test
    @DisplayName("Should throw exception for invalid method signature")
    void shouldThrowExceptionForInvalidMethodSignature() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        InvalidSignatureListener listener = new InvalidSignatureListener();

        assertThrows(InvalidEventListenerMethodSignatureException.class, () -> registry.register(listener));
    }

    @Test
    @DisplayName("Should return handlers for event type")
    void shouldReturnHandlersForEventType() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        TestAnnotatedListener listener = new TestAnnotatedListener();
        registry.register(listener);

        TestEvent event = new TestEvent();
        List<EventHandler> handlers = registry.getHandlers(event);

        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should unregister existing listener")
    void shouldUnregisterExistingListener() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        TestAnnotatedListener listener = new TestAnnotatedListener();
        registry.register(listener);

        boolean result = registry.unregister(listener);

        assertTrue(result);
        assertFalse(registry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should throw exception on merge")
    void shouldThrowExceptionOnMerge() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        SpringEventListenerRegistry otherRegistry = mock(SpringEventListenerRegistry.class);

        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    @DisplayName("Should invoke handler method")
    void shouldInvokeHandlerMethod() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        TestAnnotatedListener listener = new TestAnnotatedListener();
        registry.register(listener);

        TestEvent event = new TestEvent();
        List<EventHandler> handlers = registry.getHandlers(event);

        assertEquals(1, handlers.size());
        handlers.get(0).onEvent(event);

        assertTrue(listener.wasCalled());
    }

    static class TestAnnotatedListener {
        private boolean called = false;

        @io.github.vovten.eventflow.EventListener
        public void handleTestEvent(TestEvent event) {
            this.called = true;
        }

        boolean wasCalled() {
            return called;
        }
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    static class MultiMethodListener {
        @io.github.vovten.eventflow.EventListener
        public void handleTestEvent(TestEvent event) {
        }

        @io.github.vovten.eventflow.EventListener
        public void handleSpecificEvent(SpecificEvent event) {
        }
    }

    static class NoAnnotationListener {
        public void handleEvent(TestEvent event) {
        }
    }

    static class InvalidSignatureListener {
        @io.github.vovten.eventflow.EventListener
        public void handleEvent(String invalidParam) {
        }
    }

    static class SpecificEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return SpecificEvent.class;
        }
    }
}
