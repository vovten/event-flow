package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("AckHandler Tests")
class AckHandlerTest {

    private InMemoryEventStore eventStore;
    private AckHandler handler;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        handler = new AckHandler(eventStore, "test-service");
        eventId = UUID.randomUUID();
        StoredEvent event = StoredEvent.newEvent(eventId, "test.TestEvent", null, "{\"data\":\"test\"}", null);
        eventStore.save(event);
    }

    @Test
    @DisplayName("Should handle SuccessAck and update status to HANDLED")
    void shouldHandleSuccessAck() {
        SuccessAck ack = new SuccessAck(
                UUID.randomUUID(), eventId, "TestEvent", "test-service",
                List.of(), null, Instant.now());

        handler.onEvent(ack);

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.HANDLED);
    }

    @Test
    @DisplayName("Should handle FailureAck and update status to FAILED")
    void shouldHandleFailureAck() {
        FailureAck ack = new FailureAck(
                UUID.randomUUID(), eventId, "TestEvent", "test-service", "Handler error",
                List.of(), null, Instant.now());

        handler.onEvent(ack);

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.FAILED);
        assertThat(stored.errorDetails()).isEqualTo("Handler error");
    }

    @Test
    @DisplayName("Should silently handle missing original event")
    void shouldHandleMissingOriginalEvent() {
        SuccessAck ack = new SuccessAck(
                UUID.randomUUID(), UUID.randomUUID(), "TestEvent", "test-service",
                List.of(), null, Instant.now());

        // Should not throw
        handler.onEvent(ack);
    }

    @Test
    @DisplayName("Should declare handled event types")
    void shouldDeclareEventTypes() {
        List<Class<?>> events = handler.events();
        assertThat(events).containsExactlyInAnyOrder(SuccessAck.class, FailureAck.class);
    }

    @Test
    @DisplayName("Should have descriptive handler name")
    void shouldHaveDescriptiveName() {
        assertThat(handler.name()).isEqualTo("AckHandler");
    }

    @Test
    @DisplayName("Should skip acks from foreign service")
    void shouldSkipForeignServiceAcks() {
        SuccessAck ack = new SuccessAck(
                UUID.randomUUID(), eventId, "TestEvent", "other-service",
                List.of(), null, Instant.now());

        handler.onEvent(ack);

        // Status should remain unchanged (not HANDLED)
        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.NEW);
    }

    @Test
    @DisplayName("Should accept acks with no service configured")
    void shouldAcceptAcksWithNoService() {
        AckHandler noServiceHandler = new AckHandler(eventStore, "");

        SuccessAck ack = new SuccessAck(
                UUID.randomUUID(), eventId, "TestEvent", "some-service",
                List.of(), null, Instant.now());

        noServiceHandler.onEvent(ack);

        // Should be handled even though service doesn't match (no service configured = accept all)
        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.HANDLED);
    }

    @Nested
    @DisplayName("null originalService with configured serviceName (regression)")
    class NullOriginalServiceRegression {

        @Test
        @DisplayName("FailureAck with null originalService is accepted when serviceName is configured")
        void shouldNotSkipNullServiceAck() {
            // AckHandler with service name configured
            AckHandler handler = new AckHandler(eventStore, "test-service");

            // FailureAck where originalService is null (e.g., non-Envelope MANAGED event)
            FailureAck ack = new FailureAck(
                    UUID.randomUUID(), eventId, "TestEvent", null, "Handler error",
                    List.of(), null, Instant.now());

            handler.onEvent(ack);

            // FIXED: shouldSkipForForeignService(null) now returns false —
            // null means service info is not available, not "foreign service".
            // The event transitions to FAILED status as expected.
            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status())
                    .as("FailureAck with null originalService should update status to FAILED")
                    .isEqualTo(EventStatus.FAILED);
        }

        @Test
        @DisplayName("SuccessAck with null originalService is also accepted")
        void shouldNotSkipNullServiceSuccessAck() {
            AckHandler handler = new AckHandler(eventStore, "test-service");

            SuccessAck ack = new SuccessAck(
                    UUID.randomUUID(), eventId, "TestEvent", null,
                    List.of(), null, Instant.now());

            handler.onEvent(ack);

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status())
                    .as("SuccessAck with null originalService should update status to HANDLED")
                    .isEqualTo(EventStatus.HANDLED);
        }
    }
}
