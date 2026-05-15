package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.registry.EventListenerRegistry;
import io.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that expose defects in the current implementation.
 * These tests are expected to FAIL until the defects are fixed.
 */
@DisplayName("UnifiedEventDispatcher Defect Tests")
class UnifiedEventDispatcherDefectTest {

    private ExecutorService executor;
    private EventListenerRegistry registry;
    private InTransport transport;
    private UnifiedEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
        registry = new EventListenerRegistry();
        transport = new InTransport() {
            @Override
            public void start(java.util.function.Consumer<Event> eventConsumer) {
            }

            @Override
            public void stop() {
            }

            @Override
            public String name() {
                return "test";
            }
        };
    }

    // ============================================================
    // CRIT-2: Envelope-to-Event unwrapping breaks handlers that
    //         expect an Envelope parameter
    // ============================================================

    /**
     * Defect: When a payload implements Event, UnifiedEventDispatcher.resolveEvent()
     * unwraps it from the Envelope. But the handler was registered
     * with {@code @EventListener(DomainEvent.class)} and expects
     * {@code Envelope<DomainEvent>}. Result: ClassCastException or wrong argument type.
     */
    @Test
    @DisplayName("CRIT-2: Should pass Envelope to handler when payload is Event wrapped in Envelope")
    void shouldPassEnvelopeToHandlerWhenPayloadIsEventWrappedInEnvelope() throws Exception {
        // Arrange: handler that expects Envelope<DomainEvent>
        EnvelopeCapturingHandler handler = new EnvelopeCapturingHandler();
        registry.register(handler);

        dispatcher = new UnifiedEventDispatcher(executor, registry, List.of(transport));

        // Act: dispatch an Event payload wrapped in Envelope
        DomainEvent payload = new DomainEvent("order-123");
        Envelope<DomainEvent> envelope = Envelope.of(payload);

        CompletableFuture<HandlerResults> future = dispatcher.dispatch(envelope);
        future.get(3, TimeUnit.SECONDS);

        // Assert: handler should receive the Envelope, not the unwrapped payload
        assertThat(handler.receivedEnvelope)
                .as("CRIT-2 FAILED: Handler expecting Envelope received null. "
                        + "Bug: UnifiedEventDispatcher.resolveEvent() unwraps Event payloads "
                        + "from Envelope before dispatch, so handlers expecting Envelope "
                        + "never receive one.")
                .isNotNull();
        assertThat(handler.receivedEnvelope)
                .as("CRIT-2 FAILED: Handler expected Envelope<DomainEvent> but got " +
                        handler.receivedEvent != null ? handler.receivedEvent.getClass().getSimpleName() : "null")
                .isInstanceOf(Envelope.class);
    }

    /**
     * Same scenario but after CRIT-2 fix: handler receives Envelope, not unwrapped payload.
     */
    @Test
    @DisplayName("CRIT-2: Should pass Envelope to handler expecting Envelope (no ClassCastException)")
    void shouldPassEnvelopeToTypedHandler() throws Exception {
        // Arrange: handler with Envelope parameter
        TypedEnvelopeHandler typedHandler = new TypedEnvelopeHandler();
        registry.register(typedHandler);

        dispatcher = new UnifiedEventDispatcher(executor, registry, List.of(transport));

        // Act: dispatch Event payload wrapped in Envelope
        DomainEvent payload = new DomainEvent("order-456");
        Envelope<DomainEvent> envelope = Envelope.of(payload);

        // This should complete normally — handler receives Envelope
        CompletableFuture<HandlerResults> future = dispatcher.dispatch(envelope);

        // Assert: no exception
        HandlerResults results = future.get(3, TimeUnit.SECONDS);
        assertThat(results.isAllSuccess())
                .as("CRIT-2 FIXED: Handler expecting Envelope should complete successfully")
                .isTrue();
    }

    // ============================================================
    // HIGH-1: Envelope.type() always returns Envelope.class
    // ============================================================

    /**
     * Defect: Envelope.type() returns Envelope.class regardless of payload type.
     * This breaks logging and any code that depends on type() for event classification.
     */
    @Test
    @DisplayName("HIGH-1: Envelope.type() should reflect the payload event type, not Envelope.class")
    void envelopeTypeShouldReflectPayloadType() {
        DomainEvent payload = new DomainEvent("test");
        Envelope<DomainEvent> envelope = Envelope.of(payload);

        assertThat(envelope.type())
                .as("HIGH-1 FAILED: Envelope.type() returns %s, but it should return " +
                        "the payload's event type (or at minimum not lose the type information). " +
                        "All code using event.type().getSimpleName() will see 'Envelope' instead " +
                        "of the actual domain event type, making logs useless for debugging.",
                        envelope.type().getSimpleName())
                .isEqualTo(DomainEvent.class);
    }

    // ============================================================
    // MED-3: checkMethodSignature no longer requires Event type
    // ============================================================

    /**
     * Defect: checkMethodSignature was relaxed to allow any single-parameter method,
     * even those taking non-Event types like String or Integer.
     */
    @Test
    @DisplayName("MED-3: Should reject listener methods that accept non-Event parameters")
    void shouldRejectNonEventParameter() {
        // Arrange
        InvalidParamListener listener = new InvalidParamListener();

        // Act & Assert: The old code required Event.class.isAssignableFrom(paramType).
        // Now it accepts any single-parameter method, which is a regression.
        assertThrows(io.github.vovten.eventflow.registry.InvalidEventListenerMethodSignatureException.class,
                () -> registry.register(listener),
                "MED-3 FAILED: Listener with non-Event parameter (String) was accepted. " +
                        "checkMethodSignature no longer validates that the parameter " +
                        "extends Event, allowing potentially dangerous registrations."
        );
    }

    // ============================================================
    // Test event types
    // ============================================================

    /**
     * A domain event that implements Event interface.
     */
    public static class DomainEvent implements Event {
        private final String orderId;

        DomainEvent(String orderId) {
            this.orderId = orderId;
        }

        @Override
        public Class<? extends Event> type() {
            return DomainEvent.class;
        }

        @Override
        public String toString() {
            return "DomainEvent{orderId='" + orderId + "'}";
        }
    }

    /**
     * Handler that expects to receive an Envelope<DomainEvent>.
     */
    public static class EnvelopeCapturingHandler {
        Envelope<?> receivedEnvelope;
        Event receivedEvent;

        @EventListener(DomainEvent.class)
        public void handleOrderEvent(Envelope<DomainEvent> envelope) {
            this.receivedEnvelope = envelope;
            this.receivedEvent = envelope;
        }
    }

    /**
     * Handler that explicitly casts the parameter.
     */
    public static class TypedEnvelopeHandler {
        @EventListener(DomainEvent.class)
        public void handleOrderEvent(Envelope<DomainEvent> event) {
            // This will fail if event is not an Envelope
            DomainEvent payload = event.payload();
            if (payload == null) {
                throw new RuntimeException("payload is null");
            }
        }
    }

    /**
     * Listener with invalid parameter type (String instead of Event).
     */
    public static class InvalidParamListener {
        @EventListener
        public void handleEvent(String event) {
            // String is not an Event!
        }
    }
}
