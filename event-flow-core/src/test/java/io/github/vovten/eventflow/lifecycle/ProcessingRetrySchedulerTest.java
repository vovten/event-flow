package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.retry.InMemoryEventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.InMemoryLifecycleRepository;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.retry.RetryKind;
import io.github.vovten.eventflow.retry.RetryMetadata;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.util.EventUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ProcessingRetryScheduler Tests")
class ProcessingRetrySchedulerTest {

    private static final String SERVICE = "test-service";

    private InMemoryEventStore eventStore;
    private InMemoryEventRetryStore retryStore;
    private InMemoryLifecycleRepository lifecycleRepository;
    private RecordingPublisher origin;
    private EventPublisher lifecyclePublisher;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        retryStore = new InMemoryEventRetryStore();
        lifecycleRepository = new InMemoryLifecycleRepository(eventStore, retryStore);
        origin = new RecordingPublisher();
        lifecyclePublisher = new EventLifecyclePublisher(origin, eventStore, SERVICE);
    }

    private ProcessingRetryScheduler scheduler(int maxRetries) {
        return scheduler(options(maxRetries));
    }

    private ProcessingRetryScheduler scheduler(ProcessingRetryOptions options) {
        return new ProcessingRetryScheduler(
                retryStore, eventStore, lifecycleRepository, lifecyclePublisher, options);
    }

    private ProcessingRetryOptions options(int maxRetries) {
        return new ProcessingRetryOptions(
                SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                Duration.ofSeconds(5), 2.0, Duration.ofMinutes(1), maxRetries, 100);
    }

    private UUID storeEvent(EventStatus status) {
        UUID eventId = UUID.randomUUID();
        Envelope<TestEvent> envelope = new Envelope<>(
                eventId, null, Instant.now(), new TestEvent(eventId, "data"),
                Map.of(), List.of(ExternalEventChannel.class));
        eventStore.save(new StoredEvent(
                eventId, TestEvent.class.getName(), SERVICE, EventUtils.toJson(envelope),
                ExternalEventChannel.class.getName(), null,
                status, 0, false,
                Instant.now().minusSeconds(10), Instant.now().minusSeconds(10), "stored error"));
        return eventId;
    }

    private void scheduleProcessing(UUID eventId, int attempt) {
        retryStore.schedule(RetryTask.pending(
                eventId, SERVICE, RetryKind.PROCESSING, attempt,
                RetryReason.HANDLER_FAILED, Instant.now().minusSeconds(1), "payment"));
    }

    private void scheduleExpiredProcessing(UUID eventId, int attempt) {
        retryStore.schedule(RetryTask.pending(
                        eventId, SERVICE, RetryKind.PROCESSING, attempt,
                        RetryReason.HANDLER_FAILED, Instant.now().minusSeconds(1), "payment")
                .withClaim("other-instance", Instant.now().minusSeconds(5)));
    }

    private EventStatus status(UUID eventId) {
        return eventStore.findById(eventId).orElseThrow().status();
    }

    @Nested
    @DisplayName("retryCycle")
    class RetryCycle {

        @Test
        @DisplayName("should re-publish a due processing retry with the processing context")
        void rePublishesDueRetry() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleProcessing(eventId, 1);

            scheduler(3).retryCycle();

            assertThat(origin.published).hasSize(1);
            Event published = origin.published.get(0);
            assertThat(published).isInstanceOf(Envelope.class);
            assertThat(published.channels()).containsExactly(ExternalEventChannel.class);
            Envelope<?> envelope = (Envelope<?>) published;
            assertThat(envelope.metadata())
                    .containsEntry(RetryMetadata.RETRY_KIND, RetryKind.PROCESSING.name())
                    .containsEntry(RetryMetadata.ATTEMPT, "2");
            assertThat(status(eventId)).isEqualTo(EventStatus.PUBLISHED);
            RetryTask row = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.IN_PROGRESS);
            assertThat(row.attempt()).isEqualTo(2);
            assertThat(row.policyName()).isEqualTo("payment");
        }

        @Test
        @DisplayName("should not select delivery retries")
        void doesNotSelectDeliveryRows() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            retryStore.schedule(RetryTask.pending(
                    eventId, SERVICE, RetryKind.DELIVERY, 1,
                    RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null));

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow().state())
                    .isEqualTo(RetryState.PENDING);
        }

        @Test
        @DisplayName("should wait for the row's next attempt time")
        void respectsNextAttemptAt() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            retryStore.schedule(RetryTask.pending(
                    eventId, SERVICE, RetryKind.PROCESSING, 1,
                    RetryReason.HANDLER_FAILED, Instant.now().plusSeconds(3600), "payment"));

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
        }

        @Test
        @DisplayName("should keep the row until a final ack completes it")
        void waitsForFinalAck() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleProcessing(eventId, 1);
            scheduler(3).retryCycle();

            lifecycleRepository.onHandled(eventId, 2);

            assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
            assertThat(status(eventId)).isEqualTo(EventStatus.HANDLED);
        }

        @Test
        @DisplayName("should reschedule with backoff when the transport publish fails")
        void reschedulesAfterPublishFailure() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleProcessing(eventId, 1);
            origin.result = SendResults.of(List.of(SendResult.failure("dest", "transport down")));

            scheduler(3).retryCycle();

            RetryTask row = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.PENDING);
            assertThat(row.attempt()).isEqualTo(3);
            assertThat(row.reason()).isEqualTo(RetryReason.PUBLISH_FAILED);
            assertThat(row.nextAttemptAt()).isAfter(Instant.now());
        }

        @Test
        @DisplayName("should dead-letter without publishing when the budget is already exhausted")
        void deadLettersWhenBudgetExhausted() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleProcessing(eventId, 1);

            scheduler(0).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(status(eventId)).isEqualTo(EventStatus.DEAD);
            assertThat(retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow().state())
                    .isEqualTo(RetryState.DEAD);
        }

        @Test
        @DisplayName("should reschedule an expired claim with backoff")
        void reschedulesExpiredClaim() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleExpiredProcessing(eventId, 2);

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            RetryTask row = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.PENDING);
            assertThat(row.attempt()).isEqualTo(3);
            assertThat(row.reason()).isEqualTo(RetryReason.PROCESSING_TIMEOUT);
            assertThat(row.nextAttemptAt()).isAfter(Instant.now());
        }

        @Test
        @DisplayName("should dead-letter an expired claim when the budget is exhausted")
        void deadLettersExpiredClaimAtCap() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleExpiredProcessing(eventId, 3);

            scheduler(2).retryCycle();

            assertThat(status(eventId)).isEqualTo(EventStatus.DEAD);
            RetryTask row = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.DEAD);
            assertThat(row.reason()).isEqualTo(RetryReason.CAP_EXCEEDED);
        }

        @Test
        @DisplayName("should dead-letter when the stored event is missing")
        void deadLettersMissingStoredEvent() {
            UUID eventId = UUID.randomUUID();
            scheduleProcessing(eventId, 1);

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow().state())
                    .isEqualTo(RetryState.DEAD);
        }

        @Test
        @DisplayName("should only process rows owned by its own service")
        void onlyProcessesOwnServiceRows() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            retryStore.schedule(RetryTask.pending(
                    eventId, "other-service", RetryKind.PROCESSING, 1,
                    RetryReason.HANDLER_FAILED, Instant.now().minusSeconds(1), "payment"));

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow().state())
                    .isEqualTo(RetryState.PENDING);
        }
    }

    @Nested
    @DisplayName("options")
    class Options {

        @Test
        @DisplayName("should compute exponential backoff capped at the maximum")
        void computesBackoff() {
            ProcessingRetryOptions options = new ProcessingRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 2.0, Duration.ofSeconds(30), 3, 100);

            assertThat(options.backoff(1)).isEqualTo(Duration.ofSeconds(5));
            assertThat(options.backoff(2)).isEqualTo(Duration.ofSeconds(10));
            assertThat(options.backoff(3)).isEqualTo(Duration.ofSeconds(20));
            assertThat(options.backoff(4)).isEqualTo(Duration.ofSeconds(30));
        }

        @Test
        @DisplayName("should reject a blank service")
        void rejectsBlankService() {
            assertThatThrownBy(() -> new ProcessingRetryOptions(
                    "  ", Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 2.0, Duration.ofMinutes(1), 3, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("service must not be null or blank");
        }

        @Test
        @DisplayName("should reject a multiplier below one")
        void rejectsInvalidMultiplier() {
            assertThatThrownBy(() -> new ProcessingRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 0.5, Duration.ofMinutes(1), 3, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("multiplier must be >= 1.0");
        }

        @Test
        @DisplayName("should reject a maximum delay shorter than the base delay")
        void rejectsInvalidMaxDelay() {
            assertThatThrownBy(() -> new ProcessingRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(10), 2.0, Duration.ofSeconds(5), 3, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maxRetryDelay must not be shorter than retryDelay");
        }
    }

    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("should start and stop without errors")
        void startsAndStops() {
            ProcessingRetryScheduler scheduler = scheduler(3);
            scheduler.start();
            scheduler.stop();
        }

        @Test
        @DisplayName("should stop on close")
        void closes() {
            ProcessingRetryScheduler scheduler = scheduler(3);
            scheduler.start();
            scheduler.close();
        }
    }

    private static final class RecordingPublisher implements EventPublisher {

        private final List<Event> published = new ArrayList<>();
        private SendResults result = SendResults.of(
                List.of(SendResult.success("dest")));

        @Override
        public CompletableFuture<SendResults> publish(Event event) {
            published.add(event);
            return CompletableFuture.completedFuture(result);
        }
    }

    @io.github.vovten.eventflow.event.annotation.Event(lifecycle = EventLifecycle.MANAGED)
    private record TestEvent(UUID eventId, String data) implements Event {

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
