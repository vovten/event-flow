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

        List<EventHandler> handlers = registry.getHandlers(new TestEvent());
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should register listener with annotation value for domain event type")
    void shouldRegisterListenerWithAnnotationValueForDomainEvent() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        EnvelopeWithAnnotationListener listener = new EnvelopeWithAnnotationListener();

        registry.register(listener);

        assertEquals(1, registry.handlerCount());
    }

    @Test
    @DisplayName("Should find handler by payload type when annotated with domain event")
    void shouldFindHandlerByPayloadTypeWhenAnnotatedWithDomainEvent() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        EnvelopeWithAnnotationListener listener = new EnvelopeWithAnnotationListener();
        registry.register(listener);

        io.github.vovten.eventflow.event.Envelope<DomainOrderEvent> envelope =
                io.github.vovten.eventflow.event.Envelope.of(new DomainOrderEvent("order-123"));

        List<EventHandler> handlers = registry.getHandlers(envelope);
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should throw exception when Envelope used without annotation value")
    void shouldThrowExceptionWhenEnvelopeWithoutAnnotationValue() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(applicationContext, "com.example");
        EnvelopeWithoutAnnotationListener listener = new EnvelopeWithoutAnnotationListener();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                registry.register(listener));

        assertTrue(exception.getMessage().contains("annotation value must specify domain event type"));
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    static class TestAnnotatedListener {
        @io.github.vovten.eventflow.EventListener
        public void handleTestEvent(TestEvent event) {
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
        public void handleEvent(TestEvent event, String secondParam) {
        }
    }

    static class SpecificEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return SpecificEvent.class;
        }
    }

    static class DomainOrderEvent {
        private final String orderId;

        DomainOrderEvent(String orderId) {
            this.orderId = orderId;
        }

        String orderId() {
            return orderId;
        }
    }

    static class EnvelopeWithAnnotationListener {
        io.github.vovten.eventflow.event.Envelope<DomainOrderEvent> capturedEnvelope;

        @io.github.vovten.eventflow.EventListener(DomainOrderEvent.class)
        public void handleDomainOrderEvent(io.github.vovten.eventflow.event.Envelope<DomainOrderEvent> event) {
            this.capturedEnvelope = event;
        }
    }

    static class EnvelopeWithoutAnnotationListener {
        @io.github.vovten.eventflow.EventListener
        public void handleEnvelope(io.github.vovten.eventflow.event.Envelope<DomainOrderEvent> event) {
        }
    }
}
