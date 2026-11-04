package io.github.vovten.eventflow.lifecycle.store;

import io.github.vovten.eventflow.lifecycle.retry.InMemoryEventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.retry.RetryKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("InMemoryLifecycleRepository Tests")
class InMemoryLifecycleRepositoryTest {

    private static final String SERVICE = "test-service";

    private InMemoryEventStore eventStore;
    private InMemoryEventRetryStore retryStore;
    private InMemoryLifecycleRepository repository;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        retryStore = new InMemoryEventRetryStore();
        repository = new InMemoryLifecycleRepository(eventStore, retryStore);
        eventId = UUID.randomUUID();
        eventStore.save(StoredEvent.newEvent(
                eventId, "test.TestEvent", SERVICE, "{}", null));
    }

    @Test
    @DisplayName("Should record a publish failure and schedule delivery retry")
    void shouldRecordPublishFailure() {
        repository.onPublishFailed(eventId, SERVICE, RetryReason.PUBLISH_FAILED,
                Instant.now().plusSeconds(5), "Type", "error");

        assertThat(eventStore.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.FAILED);
        RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.PENDING);
        assertThat(task.reason()).isEqualTo(RetryReason.PUBLISH_FAILED);
    }

    @Test
    @DisplayName("Should record acceptance and clear delivery retry")
    void shouldRecordAcceptance() {
        repository.onPublishFailed(eventId, SERVICE, RetryReason.PUBLISH_FAILED,
                Instant.now().plusSeconds(5), null, null);
        eventStore.transitionStatus(eventId, java.util.Set.of(EventStatus.FAILED),
                EventStatus.PUBLISHED, null);

        repository.onAccepted(eventId, 1);

        assertThat(eventStore.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.ACCEPTED);
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
    }

    @Test
    @DisplayName("Should keep FAILED when acceptance arrives late")
    void shouldKeepFailedOnLateAcceptance() {
        eventStore.transitionStatus(eventId, java.util.Set.of(EventStatus.NEW),
                EventStatus.FAILED, "error");

        repository.onAccepted(eventId, 1);

        assertThat(eventStore.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.FAILED);
    }

    @Test
    @DisplayName("Should record retryable processing failure")
    void shouldRecordRetryableProcessingFailure() {
        repository.onProcessingFailed(eventId, SERVICE, 2, true,
                Instant.now().plusSeconds(10), "payment", "Type", "error");

        assertThat(eventStore.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.FAILED);
        RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.PENDING);
        assertThat(task.attempt()).isEqualTo(2);
        assertThat(task.policyName()).isEqualTo("payment");
    }

    @Test
    @DisplayName("Should record permanent processing failure as dead")
    void shouldRecordPermanentProcessingFailure() {
        repository.onProcessingFailed(eventId, SERVICE, 1, false,
                Instant.now(), null, "Type", "error");

        assertThat(eventStore.findById(eventId).orElseThrow().status())
                .isEqualTo(EventStatus.DEAD);
        RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.DEAD);
    }

    @Test
    @DisplayName("Should not create retry rows for a healthy lifecycle")
    void shouldNotCreateRetryRowsForHealthyEvent() {
        repository.onAccepted(eventId, 1);
        repository.onHandled(eventId, 1);

        assertThat(eventStore.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.HANDLED);
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
    }

    @Test
    @DisplayName("Should record successful handling and clear retries")
    void shouldRecordHandled() {
        repository.onProcessingFailed(eventId, SERVICE, 1, true,
                Instant.now().plusSeconds(10), null, null, "error");

        repository.onHandled(eventId, 2);

        assertThat(eventStore.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.HANDLED);
        assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
    }

    @Test
    @DisplayName("Should record no-handler as dead")
    void shouldRecordNoHandler() {
        repository.onNoHandler(eventId, SERVICE, 1, "Type", "no handler");

        assertThat(eventStore.findById(eventId).orElseThrow().status())
                .isEqualTo(EventStatus.DEAD);
        RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.DEAD);
        assertThat(task.reason()).isEqualTo(RetryReason.NO_HANDLER);
    }

    @Test
    @DisplayName("Should dead-letter an explicit retry kind")
    void shouldDeadLetterExplicitKind() {
        repository.onDeadLetter(eventId, SERVICE, RetryKind.DELIVERY,
                RetryReason.ACK_TIMEOUT, "Type", "timeout");

        assertThat(eventStore.findById(eventId).orElseThrow().status())
                .isEqualTo(EventStatus.DEAD);
        RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.DEAD);
        assertThat(task.reason()).isEqualTo(RetryReason.ACK_TIMEOUT);
    }

    @Test
    @DisplayName("Should reset an event for manual retry")
    void shouldMarkForRetry() {
        repository.onProcessingFailed(eventId, SERVICE, 1, false,
                Instant.now(), "payment", "Type", "error");

        repository.markForRetry(eventId, SERVICE);

        assertThat(eventStore.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.NEW);
        RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.PENDING);
        assertThat(task.attempt()).isEqualTo(1);
        assertThat(task.policyName()).isEqualTo("payment");
    }
}
