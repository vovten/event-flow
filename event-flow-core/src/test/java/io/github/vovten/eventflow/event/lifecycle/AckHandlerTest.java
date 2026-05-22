package io.github.vovten.eventflow.event.lifecycle;

import io.github.vovten.eventflow.store.EventStatus;
import io.github.vovten.eventflow.store.InMemoryEventStore;
import io.github.vovten.eventflow.store.StoredEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
        StoredEvent event = StoredEvent.newEvent(eventId, "test.TestEvent", "{\"data\":\"test\"}", null);
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
    @DisplayName("Should handle FailureAck and update status to HANDLE_FAILED")
    void shouldHandleFailureAck() {
        FailureAck ack = new FailureAck(
                UUID.randomUUID(), eventId, "TestEvent", "test-service", "Handler error",
                List.of(), null, Instant.now());

        handler.onEvent(ack);

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.HANDLE_FAILED);
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
}
