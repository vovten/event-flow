package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Tests for {@link EventListenerRegistry}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("EventListenerRegistry Tests")
class EventListenerRegistryTest {

    private EventListenerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new EventListenerRegistry();
    }

    @Test
    @DisplayName("Should register listener with annotated method")
    void shouldRegisterListenerWithAnnotatedMethod() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();

        // Act
        registry.register(listener);

        // Assert
        var handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should register listener with multiple annotated methods")
    void shouldRegisterListenerWithMultipleAnnotatedMethods() {
        // Arrange
        MultiMethodListener listener = new MultiMethodListener();

        // Act
        registry.register(listener);

        // Assert
        assertEquals(2, registry.handlerCount());
    }

    @Test
    @DisplayName("Should register generic listener for Event.class")
    void shouldRegisterGenericListenerForEventClass() {
        // Arrange
        GenericListener listener = new GenericListener();

        // Act
        registry.register(listener);

        // Assert
        var handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should throw exception for invalid method signature (no params)")
    void shouldThrowExceptionForInvalidMethodSignature() {
        // Arrange
        NoParamsListener listener = new NoParamsListener();

        // Assert
        assertThrows(InvalidEventListenerMethodSignatureException.class, () ->
                registry.register(listener)
        );
    }

    @Test
    @DisplayName("Should register listener for POJO payload that does not implement Event")
    void shouldRegisterListenerForPojoPayload() {
        // Arrange
        PojoListener listener = new PojoListener();

        // Act
        registry.register(listener);
        PojoEvent pojoEvent = new PojoEvent("test-id");
        UUID processId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, processId);

        // Assert
        var handlers = registry.getHandlers(envelope);
        assertEquals(1, handlers.size());

        // Verify onEvent unwraps and passes POJO payload
        assertFalse(handlers.isEmpty());
        handlers.forEach(handler -> handler.onEvent(envelope));
        assertNotNull(listener.capturedPayload);
        assertTrue(listener.capturedPayload instanceof PojoEvent);
        assertEquals("test-id", ((PojoEvent) listener.capturedPayload).getId());
    }

    @Test
    @DisplayName("Should unregister listener")
    void shouldUnregisterListener() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();
        registry.register(listener);

        // Act
        boolean unregistered = registry.unregister(listener);

        // Assert
        assertTrue(unregistered);
        assertTrue(registry.getHandlers(new TestEvent("test")).isEmpty());
    }

    @Test
    @DisplayName("Should return false when unregistering non-existent listener")
    void shouldReturnFalseWhenUnregisteringNonExistentListener() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();

        // Act
        boolean unregistered = registry.unregister(listener);

        // Assert
        assertFalse(unregistered);
    }

    @Test
    @DisplayName("Should check if listener is registered")
    void shouldCheckIfListenerIsRegistered() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();
        registry.register(listener);

        // Act & Assert
        assertTrue(registry.isRegistered(listener));
        assertFalse(registry.isRegistered(new Object()));
    }

    @Test
    @DisplayName("Should throw exception for merge operation")
    void shouldThrowExceptionForMergeOperation() {
        // Arrange
        EventSubscriberRegistry otherRegistry = new EventSubscriberRegistry();

        // Assert
        assertThrows(UnsupportedOperationException.class, () ->
                registry.merge(otherRegistry)
        );
    }

    @Test
    @DisplayName("Should not register duplicate listener")
    void shouldNotRegisterDuplicateListener() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();

        // Act
        registry.register(listener);
        registry.register(listener);

        // Assert
        var handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should resolve handlers for Envelope wrapping specific event type")
    void shouldResolveHandlersForEnvelopeWrappingEventType() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();
        registry.register(listener);
        TestEvent testEvent = new TestEvent("test");
        Envelope<TestEvent> envelope = Envelope.of(testEvent);

        // Act
        var handlers = registry.getHandlers(envelope);

        // Assert
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should resolve generic handlers for Envelope when no specific handlers exist")
    void shouldResolveGenericHandlersForEnvelope() {
        // Arrange
        GenericListener genericListener = new GenericListener();
        registry.register(genericListener);
        TestEvent testEvent = new TestEvent("test");
        Envelope<TestEvent> envelope = Envelope.of(testEvent);

        // Act
        var handlers = registry.getHandlers(envelope);

        // Assert
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should invoke handler with unwrapped payload when event is Envelope")
    void shouldInvokeHandlerWithUnwrappedPayloadWhenEventIsEnvelope() {
        // Arrange
        CapturingPayloadListener listener = new CapturingPayloadListener();
        registry.register(listener);
        TestEvent testEvent = new TestEvent("payload-data");
        UUID processId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Envelope<TestEvent> envelope = Envelope.of(testEvent, processId);

        // Act
        var handlers = registry.getHandlers(envelope);
        assertFalse(handlers.isEmpty());
        handlers.forEach(handler -> handler.onEvent(envelope));

        // Assert
        assertNotNull(listener.capturedEvent);
        assertTrue(listener.capturedEvent instanceof TestEvent);
        assertFalse(listener.capturedEvent instanceof Envelope);
        assertEquals("payload-data", ((TestEvent) listener.capturedEvent).getData());
    }

    @Test
    @DisplayName("Should resolve both specific and generic handlers for Envelope")
    void shouldResolveBothSpecificAndGenericHandlersForEnvelope() {
        // Arrange
        AnnotatedListener specificListener = new AnnotatedListener();
        GenericListener genericListener = new GenericListener();
        registry.register(specificListener);
        registry.register(genericListener);
        TestEvent testEvent = new TestEvent("test");
        Envelope<TestEvent> envelope = Envelope.of(testEvent);

        // Act
        var handlers = registry.getHandlers(envelope);

        // Assert
        assertEquals(2, handlers.size());
    }

    @Test
    @DisplayName("Should register listener with annotation value and receive Envelope")
    void shouldRegisterListenerWithAnnotationValueReceivingEnvelope() {
        // Arrange
        EnvelopeReceivingListener listener = new EnvelopeReceivingListener();
        registry.register(listener);
        DomainOrderEvent orderEvent = new DomainOrderEvent("order-123");
        Envelope<DomainOrderEvent> envelope = Envelope.of(orderEvent);

        // Act
        var handlers = registry.getHandlers(envelope);

        // Assert
        assertEquals(1, handlers.size());
        handlers.forEach(handler -> handler.onEvent(envelope));
        assertNotNull(listener.capturedEnvelope);
        assertEquals("order-123", listener.capturedEnvelope.payload().orderId());
    }

    @Test
    @DisplayName("Should register listener with annotation value for domain event and find handler by payload type")
    void shouldFindHandlerByPayloadTypeWhenAnnotatedWithDomainEvent() {
        // Arrange
        EnvelopeReceivingListener listener = new EnvelopeReceivingListener();
        registry.register(listener);
        DomainOrderEvent orderEvent = new DomainOrderEvent("order-456");
        Envelope<DomainOrderEvent> envelope = Envelope.of(orderEvent);

        // Act
        var handlers = registry.getHandlers(envelope);

        // Assert
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should throw exception when Envelope used without annotation value")
    void shouldThrowExceptionWhenEnvelopeWithoutAnnotationValue() {
        // Arrange
        EnvelopeWithoutAnnotationListener listener = new EnvelopeWithoutAnnotationListener();

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                registry.register(listener));

        assertTrue(exception.getMessage().contains("Listener 'EnvelopeWithoutAnnotationListener.handleEnvelope()':"));
    }

    /**
     * Test event class.
     */
    private static final class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
            this.data = data;
        }

        String getData() {
            return data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\"}";
        }
    }

    /**
     * Another test event class.
     */
    private static final class AnotherEvent extends AbstractTraceableEvent {
        private final String data;

        AnotherEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return AnotherEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\"}";
        }
    }

    /**
     * Listener with annotated method.
     */
    private static final class AnnotatedListener {
        @EventListener
        public void handleTestEvent(TestEvent event) {
        }
    }

    /**
     * Listener with multiple annotated methods.
     */
    private static final class MultiMethodListener {
        @EventListener
        public void handleTestEvent(TestEvent event) {
        }

        @EventListener
        public void handleAnotherEvent(AnotherEvent event) {
        }
    }

    /**
     * Generic listener for all events.
     */
    private static final class GenericListener {
        @EventListener
        public void handleEvent(Event event) {
        }
    }

    /**
     * Listener with invalid method signature (no parameters).
     */
    private static final class NoParamsListener {
        @EventListener
        public void handleWithNoParams() {
        }
    }

    /**
     * Listener for POJO payloads annotated with {@code @Event}.
     */
    static final class PojoListener {
        Object capturedPayload;

        @EventListener
        public void handlePojoEvent(PojoEvent event) {
            this.capturedPayload = event;
        }
    }

    /**
     * POJO event class annotated with {@code @Event}.
     */
    @io.github.vovten.eventflow.event.annotation.Event
    static final class PojoEvent {
        private final String id;

        PojoEvent(String id) {
            this.id = id;
        }

        String getId() {
            return id;
        }
    }

    /**
     * Listener that captures received event for testing.
     */
    static final class CapturingPayloadListener {
        Event capturedEvent;

        @EventListener
        public void handleTestEvent(TestEvent event) {
            this.capturedEvent = event;
        }
    }

    /**
     * Domain order event for testing annotation value.
     */
    static final class DomainOrderEvent {
        private final String orderId;

        DomainOrderEvent(String orderId) {
            this.orderId = orderId;
        }

        String orderId() {
            return orderId;
        }
    }

    /**
     * Listener that receives Envelope with @EventListener(DomainOrderEvent.class) annotation.
     */
    static final class EnvelopeReceivingListener {
        Envelope<DomainOrderEvent> capturedEnvelope;

        @EventListener(DomainOrderEvent.class)
        public void handleDomainOrderEvent(Envelope<DomainOrderEvent> event) {
            this.capturedEnvelope = event;
        }
    }

    /**
     * Listener that receives Envelope WITHOUT annotation value - should throw exception.
     */
    static final class EnvelopeWithoutAnnotationListener {
        @EventListener
        public void handleEnvelope(Envelope<DomainOrderEvent> event) {
        }
    }
}

