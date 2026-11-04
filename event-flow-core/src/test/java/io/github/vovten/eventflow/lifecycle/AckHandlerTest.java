package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.retry.RetryKind;
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
    @DisplayName("Should handle AcceptedAck and update status to ACCEPTED")
    void shouldHandleAcceptedAck() {
        handler.onEvent(acceptedAck());

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.ACCEPTED);
    }

    @Test
    @DisplayName("Should handle NoHandlerAck and update status to DEAD")
    void shouldHandleNoHandlerAck() {
        handler.onEvent(noHandlerAck());

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.DEAD);
        assertThat(stored.errorDetails()).contains("No handler resolved");
    }

    @Test
    @DisplayName("Should handle a non-retryable FailureAck as DEAD")
    void shouldHandleNonRetryableFailureAckAsDead() {
        handler.onEvent(failureAck(false));

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.DEAD);
        assertThat(stored.errorDetails()).isEqualTo("Handler error");
    }

    @Test
    @DisplayName("Should not overwrite FAILED with a late AcceptedAck")
    void shouldNotOverwriteFailedWithLateAcceptedAck() {
        handler.onEvent(failureAck(true));
        handler.onEvent(acceptedAck());

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.FAILED);
    }

    @Test
    @DisplayName("Should not overwrite HANDLED with a late AcceptedAck")
    void shouldNotOverwriteHandledWithLateAcceptedAck() {
        handler.onEvent(new SuccessAck(
                UUID.randomUUID(), eventId, "TestEvent", "test-service",
                List.of(), null, Instant.now()));
        handler.onEvent(acceptedAck());

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.HANDLED);
    }

    @Test
    @DisplayName("Should move ACCEPTED to DEAD on NoHandlerAck")
    void shouldMoveAcceptedToDeadOnNoHandlerAck() {
        handler.onEvent(acceptedAck());
        handler.onEvent(noHandlerAck());

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.DEAD);
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
        assertThat(events).containsExactlyInAnyOrder(
                AcceptedAck.class, NoHandlerAck.class, SuccessAck.class, FailureAck.class);
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

    private AcceptedAck acceptedAck() {
        return new AcceptedAck(
                UUID.randomUUID(), eventId, "TestEvent", "test-service",
                1, 0L, List.of(), Instant.now());
    }

    private NoHandlerAck noHandlerAck() {
        return new NoHandlerAck(
                UUID.randomUUID(), eventId, "TestEvent", "test-service",
                1, List.of(), Instant.now());
    }

    private FailureAck failureAck(boolean retryable) {
        return new FailureAck(
                UUID.randomUUID(), eventId, "TestEvent", "test-service", "Handler error",
                List.of(), null, Instant.now(),
                RetryKind.PROCESSING, 1, retryable, List.of(), null, 0L, null);
    }
}
