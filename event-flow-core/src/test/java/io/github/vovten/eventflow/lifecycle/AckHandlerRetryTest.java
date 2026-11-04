package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.retry.InMemoryEventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.InMemoryLifecycleRepository;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.retry.RetryKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("AckHandler Retry-Aware Tests")
class AckHandlerRetryTest {

    private static final String SERVICE = "test-service";

    private InMemoryEventStore eventStore;
    private InMemoryEventRetryStore retryStore;
    private InMemoryLifecycleRepository lifecycleRepository;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        retryStore = new InMemoryEventRetryStore();
        lifecycleRepository = new InMemoryLifecycleRepository(eventStore, retryStore);
    }

    private AckHandler handler() {
        return handler(true, 3);
    }

    private AckHandler handler(boolean processingEnabled, int maxRetries) {
        return new AckHandler(lifecycleRepository, retryStore, SERVICE,
                new AckHandlerOptions(processingEnabled,
                        Duration.ofSeconds(30), Duration.ofMinutes(10), maxRetries));
    }

    private UUID storeEvent(EventStatus status) {
        UUID eventId = UUID.randomUUID();
        eventStore.save(StoredEvent.newEvent(
                eventId, "test.TestEvent", SERVICE, "{}", null, status));
        return eventId;
    }

    private void scheduleDelivery(UUID eventId, int attempt) {
        retryStore.schedule(RetryTask.pending(
                eventId, SERVICE, RetryKind.DELIVERY, attempt,
                RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null));
    }

    private void scheduleProcessing(UUID eventId, int attempt) {
        retryStore.schedule(RetryTask.pending(
                eventId, SERVICE, RetryKind.PROCESSING, attempt,
                RetryReason.HANDLER_FAILED, Instant.now().minusSeconds(1), "payment"));
    }

    private EventStatus status(UUID eventId) {
        return eventStore.findById(eventId).orElseThrow().status();
    }

    private AcceptedAck acceptedAck(UUID eventId, int attempt) {
        return new AcceptedAck(UUID.randomUUID(), eventId, "TestEvent", SERVICE,
                attempt, 0L, List.of(), Instant.now());
    }

    private NoHandlerAck noHandlerAck(UUID eventId, int attempt) {
        return new NoHandlerAck(UUID.randomUUID(), eventId, "TestEvent", SERVICE,
                attempt, List.of(), Instant.now());
    }

    private SuccessAck successAck(UUID eventId, RetryKind retryKind, int attempt) {
        return new SuccessAck(UUID.randomUUID(), eventId, "TestEvent", SERVICE,
                List.of(), null, Instant.now(), retryKind, attempt);
    }

    private FailureAck failureAck(UUID eventId, RetryKind retryKind, int attempt,
                                  boolean retryable, long retryAfterMs, String policyName) {
        return new FailureAck(UUID.randomUUID(), eventId, "TestEvent", SERVICE, "Handler error",
                List.of(), null, Instant.now(), retryKind, attempt, retryable,
                List.of("handler"), "java.lang.RuntimeException", retryAfterMs, policyName);
    }

    @Test
    @DisplayName("should ignore acks from a foreign service")
    void ignoresForeignService() {
        UUID eventId = storeEvent(EventStatus.PUBLISHED);
        SuccessAck ack = new SuccessAck(UUID.randomUUID(), eventId, "TestEvent",
                "other-service", List.of(), null, Instant.now(),
                RetryKind.PROCESSING, 1);

        handler().onEvent(ack);

        assertThat(status(eventId)).isEqualTo(EventStatus.PUBLISHED);
    }

    @Nested
    @DisplayName("AcceptedAck")
    class Accepted {

        @Test
        @DisplayName("should move a new event to ACCEPTED")
        void movesNewEventToAccepted() {
            UUID eventId = storeEvent(EventStatus.NEW);

            handler().onEvent(acceptedAck(eventId, 1));

            assertThat(status(eventId)).isEqualTo(EventStatus.ACCEPTED);
        }

        @Test
        @DisplayName("should clear the delivery row")
        void clearsDeliveryRow() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleDelivery(eventId, 1);

            handler().onEvent(acceptedAck(eventId, 1));

            assertThat(status(eventId)).isEqualTo(EventStatus.ACCEPTED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }

        @Test
        @DisplayName("should keep FAILED status but clear the delivery row")
        void keepsFailedStatus() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleDelivery(eventId, 1);

            handler().onEvent(acceptedAck(eventId, 1));

            assertThat(status(eventId)).isEqualTo(EventStatus.FAILED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }

        @Test
        @DisplayName("should ignore a stale delivery attempt")
        void ignoresStaleAttempt() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleDelivery(eventId, 3);

            handler().onEvent(acceptedAck(eventId, 1));

            assertThat(status(eventId)).isEqualTo(EventStatus.PUBLISHED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isPresent();
        }

        @Test
        @DisplayName("should not roll back HANDLED but should clear the delivery row")
        void doesNotRollBackHandled() {
            UUID eventId = storeEvent(EventStatus.HANDLED);
            scheduleDelivery(eventId, 1);

            handler().onEvent(acceptedAck(eventId, 1));

            assertThat(status(eventId)).isEqualTo(EventStatus.HANDLED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }
    }

    @Nested
    @DisplayName("SuccessAck")
    class Success {

        @Test
        @DisplayName("should move the event to HANDLED and clear both rows")
        void movesToHandled() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleDelivery(eventId, 1);
            scheduleProcessing(eventId, 1);

            handler().onEvent(successAck(eventId, RetryKind.PROCESSING, 1));

            assertThat(status(eventId)).isEqualTo(EventStatus.HANDLED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
        }

        @Test
        @DisplayName("should close a delivery context success and mark HANDLED")
        void closesDeliveryContext() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleDelivery(eventId, 2);

            handler().onEvent(successAck(eventId, RetryKind.DELIVERY, 2));

            assertThat(status(eventId)).isEqualTo(EventStatus.HANDLED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }

        @Test
        @DisplayName("should ignore a stale processing success")
        void ignoresStaleProcessingSuccess() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleProcessing(eventId, 3);

            handler().onEvent(successAck(eventId, RetryKind.PROCESSING, 1));

            assertThat(status(eventId)).isEqualTo(EventStatus.FAILED);
            assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isPresent();
        }
    }

    @Nested
    @DisplayName("FailureAck")
    class Failure {

        @Test
        @DisplayName("should record a retryable processing failure with the ack delay and policy")
        void recordsRetryableFailure() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED);

            handler().onEvent(failureAck(eventId, RetryKind.PROCESSING, 2,
                    true, 5000L, "payment"));

            assertThat(status(eventId)).isEqualTo(EventStatus.FAILED);
            RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.PENDING);
            assertThat(task.attempt()).isEqualTo(2);
            assertThat(task.policyName()).isEqualTo("payment");
            assertThat(task.nextAttemptAt()).isAfter(Instant.now().plusSeconds(4));
        }

        @Test
        @DisplayName("should use the default delay when the ack has no retry delay")
        void usesDefaultDelay() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED);

            handler(true, 3).onEvent(failureAck(eventId, RetryKind.PROCESSING, 1,
                    true, 0L, null));

            RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(task.nextAttemptAt()).isAfter(Instant.now().plusSeconds(25));
        }

        @Test
        @DisplayName("should dead-letter a permanent processing failure")
        void deadLettersPermanentFailure() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED);

            handler().onEvent(failureAck(eventId, RetryKind.PROCESSING, 1,
                    false, 0L, null));

            assertThat(status(eventId)).isEqualTo(EventStatus.DEAD);
            RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.DEAD);
        }

        @Test
        @DisplayName("should dead-letter when the global processing budget is exhausted")
        void deadLettersWhenBudgetExhausted() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED);

            handler(true, 1).onEvent(failureAck(eventId, RetryKind.PROCESSING, 2,
                    true, 0L, "payment"));

            assertThat(status(eventId)).isEqualTo(EventStatus.DEAD);
            assertThat(retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow().state())
                    .isEqualTo(RetryState.DEAD);
        }

        @Test
        @DisplayName("should clear delivery and start processing from attempt one for a delivery context")
        void deliveryContextStartsProcessingAtOne() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleDelivery(eventId, 2);

            handler().onEvent(failureAck(eventId, RetryKind.DELIVERY, 2,
                    true, 0L, null));

            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
            RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.PENDING);
            assertThat(task.attempt()).isEqualTo(1);
        }

        @Test
        @DisplayName("should record FAILED without a processing row when processing is disabled")
        void processingDisabledRecordsFailedOnly() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED);
            scheduleDelivery(eventId, 1);

            handler(false, 3).onEvent(failureAck(eventId, RetryKind.PROCESSING, 1,
                    true, 0L, null));

            assertThat(status(eventId)).isEqualTo(EventStatus.FAILED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
        }

        @Test
        @DisplayName("should ignore a stale processing failure")
        void ignoresStaleProcessingFailure() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleProcessing(eventId, 4);

            handler().onEvent(failureAck(eventId, RetryKind.PROCESSING, 2,
                    true, 0L, null));

            RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(task.attempt()).isEqualTo(4);
            assertThat(task.state()).isEqualTo(RetryState.PENDING);
        }
    }

    @Nested
    @DisplayName("NoHandlerAck")
    class NoHandler {

        @Test
        @DisplayName("should dead-letter the event and clear the delivery row")
        void deadLettersEvent() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED);
            scheduleDelivery(eventId, 1);

            handler().onEvent(noHandlerAck(eventId, 1));

            assertThat(status(eventId)).isEqualTo(EventStatus.DEAD);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
            RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.DEAD);
            assertThat(task.reason()).isEqualTo(RetryReason.NO_HANDLER);
        }
    }

}
