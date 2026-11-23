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

@DisplayName("DeliveryRetryScheduler Tests")
class DeliveryRetrySchedulerTest {

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

    private DeliveryRetryScheduler scheduler(int maxRetries) {
        return scheduler(options(maxRetries));
    }

    private DeliveryRetryScheduler scheduler(DeliveryRetryOptions options) {
        return new DeliveryRetryScheduler(
                retryStore, eventStore, lifecycleRepository, lifecyclePublisher, options);
    }

    private DeliveryRetryOptions options(int maxRetries) {
        return new DeliveryRetryOptions(
                SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                Duration.ofSeconds(5), 2.0, 0.0, Duration.ofMinutes(1), maxRetries, 100);
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

    private UUID storeEventWithMissingType() {
        UUID eventId = UUID.randomUUID();
        Envelope<TestEvent> envelope = new Envelope<>(
                eventId, null, Instant.now(), new TestEvent(eventId, "data"),
                Map.of(), List.of(ExternalEventChannel.class));
        String payload = EventUtils.toJson(envelope)
                .replace(TestEvent.class.getName(), "com.example.MissingEvent");
        eventStore.save(new StoredEvent(
                eventId, TestEvent.class.getName(), SERVICE, payload,
                ExternalEventChannel.class.getName(), null,
                EventStatus.FAILED, 0, false,
                Instant.now().minusSeconds(10), Instant.now().minusSeconds(10), "stored error"));
        return eventId;
    }

    private void scheduleDelivery(UUID eventId, int attempt) {
        retryStore.schedule(RetryTask.pending(
                eventId, SERVICE, RetryKind.DELIVERY, attempt,
                RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null));
    }

    private void scheduleExpiredDelivery(UUID eventId, int attempt) {
        retryStore.schedule(RetryTask.pending(
                        eventId, SERVICE, RetryKind.DELIVERY, attempt,
                        RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null)
                .withClaim("other-instance", Instant.now().minusSeconds(5)));
    }

    private EventStatus status(UUID eventId) {
        return eventStore.findById(eventId).orElseThrow().status();
    }

    @Nested
    @DisplayName("retryCycle")
    class RetryCycle {

        @Test
        @DisplayName("should re-publish a due delivery retry with the delivery context")
        void rePublishesDueRetry() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleDelivery(eventId, 1);

            scheduler(3).retryCycle();

            assertThat(origin.published).hasSize(1);
            Event published = origin.published.get(0);
            assertThat(published).isInstanceOf(Envelope.class);
            assertThat(published.channels()).containsExactly(ExternalEventChannel.class);
            Envelope<?> envelope = (Envelope<?>) published;
            assertThat(envelope.metadata())
                    .containsEntry(RetryMetadata.RETRY_KIND, RetryKind.DELIVERY.name())
                    .containsEntry(RetryMetadata.ATTEMPT, "2");
            assertThat(status(eventId)).isEqualTo(EventStatus.PUBLISHED);
            RetryTask row = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.IN_PROGRESS);
            assertThat(row.claimedUntil()).isNotNull();
        }

        @Test
        @DisplayName("should wrap a raw event so the retry context is transported")
        void wrapsRawEventWithRetryContext() {
            UUID eventId = UUID.randomUUID();
            eventStore.save(new StoredEvent(
                    eventId, TestEvent.class.getName(), SERVICE,
                    EventUtils.toJson(new TestEvent(eventId, "data")), null, null,
                    EventStatus.FAILED, 0, false,
                    Instant.now().minusSeconds(10), Instant.now().minusSeconds(10), "stored error"));
            scheduleDelivery(eventId, 1);

            scheduler(3).retryCycle();

            assertThat(origin.published).hasSize(1);
            Event published = origin.published.get(0);
            assertThat(published).isInstanceOf(Envelope.class);
            Envelope<?> envelope = (Envelope<?>) published;
            assertThat(envelope.eventId()).isEqualTo(eventId);
            assertThat(envelope.metadata())
                    .containsEntry(RetryMetadata.RETRY_KIND, RetryKind.DELIVERY.name())
                    .containsEntry(RetryMetadata.ATTEMPT, "2");
        }

        @Test
        @DisplayName("should not select processing retries")
        void doesNotSelectProcessingRows() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            retryStore.schedule(RetryTask.pending(
                    eventId, SERVICE, RetryKind.PROCESSING, 1,
                    RetryReason.HANDLER_FAILED, Instant.now().minusSeconds(1), "payment"));

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow().state())
                    .isEqualTo(RetryState.PENDING);
        }

        @Test
        @DisplayName("should ignore accepted events without a delivery row")
        void ignoresAcceptedEventWithoutDeliveryRow() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED);

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }

        @Test
        @DisplayName("should reschedule when the transport publish fails")
        void reschedulesAfterPublishFailure() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleDelivery(eventId, 1);
            origin.result = SendResults.of(List.of(SendResult.failure("dest", "transport down")));

            scheduler(3).retryCycle();

            assertThat(status(eventId)).isEqualTo(EventStatus.FAILED);
            RetryTask row = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.PENDING);
            assertThat(row.attempt()).isEqualTo(3);
            assertThat(row.reason()).isEqualTo(RetryReason.PUBLISH_FAILED);
            assertThat(row.nextAttemptAt())
                    .as("publish failure must apply the retry backoff, not retry on every cycle")
                    .isAfter(Instant.now().plusSeconds(5));
        }

        @Test
        @DisplayName("should dead-letter without publishing when the budget is already exhausted")
        void deadLettersWhenBudgetExhausted() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleDelivery(eventId, 1);

            scheduler(0).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(status(eventId)).isEqualTo(EventStatus.DEAD);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow().state())
                    .isEqualTo(RetryState.DEAD);
        }

        @Test
        @DisplayName("should reschedule an expired claim and increment the attempt")
        void reschedulesExpiredClaim() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleExpiredDelivery(eventId, 1);

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            RetryTask row = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.PENDING);
            assertThat(row.attempt()).isEqualTo(2);
            assertThat(row.reason()).isEqualTo(RetryReason.ACK_TIMEOUT);
        }

        @Test
        @DisplayName("should dead-letter an expired claim when the budget is exhausted")
        void deadLettersExpiredClaimAtCap() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED);
            scheduleExpiredDelivery(eventId, 3);

            scheduler(2).retryCycle();

            assertThat(status(eventId)).isEqualTo(EventStatus.DEAD);
            RetryTask row = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.DEAD);
            assertThat(row.reason()).isEqualTo(RetryReason.CAP_EXCEEDED);
        }

        @Test
        @DisplayName("should dead-letter when the stored event is missing")
        void deadLettersMissingStoredEvent() {
            UUID eventId = UUID.randomUUID();
            scheduleDelivery(eventId, 1);

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow().state())
                    .isEqualTo(RetryState.DEAD);
        }

        @Test
        @DisplayName("should keep the row until an acceptance ack completes it")
        void waitsForAcceptanceAck() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            scheduleDelivery(eventId, 1);
            scheduler(3).retryCycle();

            lifecycleRepository.onAccepted(eventId, 1);

            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
            assertThat(status(eventId)).isEqualTo(EventStatus.ACCEPTED);
        }

        @Test
        @DisplayName("should honour the batch size")
        void honoursBatchSize() {
            UUID first = storeEvent(EventStatus.FAILED);
            UUID second = storeEvent(EventStatus.FAILED);
            UUID third = storeEvent(EventStatus.FAILED);
            scheduleDelivery(first, 1);
            scheduleDelivery(second, 1);
            scheduleDelivery(third, 1);
            DeliveryRetryOptions limited = new DeliveryRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 2.0, 0.0, Duration.ofMinutes(1), 3, 2);

            scheduler(limited).retryCycle();

            assertThat(origin.published).hasSize(2);
            long pending = List.of(first, second, third).stream()
                    .map(id -> retryStore.find(id, RetryKind.DELIVERY).orElseThrow())
                    .filter(row -> row.state() == RetryState.PENDING)
                    .count();
            assertThat(pending).isEqualTo(1);
        }

        @Test
        @DisplayName("should only process rows owned by its own service")
        void onlyProcessesOwnServiceRows() {
            UUID eventId = storeEvent(EventStatus.FAILED);
            retryStore.schedule(RetryTask.pending(
                    eventId, "other-service", RetryKind.DELIVERY, 1,
                    RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null));

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow().state())
                    .isEqualTo(RetryState.PENDING);
        }

        @Test
        @DisplayName("should dead-letter an invalid payload without burning the budget")
        void deadLettersInvalidPayload() {
            UUID invalidId = UUID.randomUUID();
            eventStore.save(new StoredEvent(
                    invalidId, TestEvent.class.getName(), SERVICE, "{invalid-json", null, null,
                    EventStatus.FAILED, 0, false,
                    Instant.now().minusSeconds(20), Instant.now().minusSeconds(20), "stored error"));
            scheduleDelivery(invalidId, 1);
            UUID validId = storeEvent(EventStatus.FAILED);
            scheduleDelivery(validId, 1);

            scheduler(3).retryCycle();

            assertThat(origin.published).hasSize(1);
            assertThat(status(invalidId)).isEqualTo(EventStatus.DEAD);
            RetryTask invalidRow = retryStore.find(invalidId, RetryKind.DELIVERY).orElseThrow();
            assertThat(invalidRow.state()).isEqualTo(RetryState.DEAD);
            assertThat(invalidRow.reason()).isEqualTo(RetryReason.PAYLOAD_UNREADABLE);
            assertThat(retryStore.find(validId, RetryKind.DELIVERY).orElseThrow().state())
                    .isEqualTo(RetryState.IN_PROGRESS);
        }

        @Test
        @DisplayName("should retry a transient payload failure instead of dead-lettering immediately")
        void retriesTransientPayloadFailure() {
            UUID eventId = storeEventWithMissingType();
            scheduleDelivery(eventId, 1);

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(status(eventId)).isEqualTo(EventStatus.FAILED);
            RetryTask row = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.PENDING);
            assertThat(row.reason()).isEqualTo(RetryReason.PAYLOAD_UNREADABLE);
            assertThat(row.attempt()).isEqualTo(3);
            assertThat(row.lastError()).contains("EventSerializationException");
        }

        @Test
        @DisplayName("should dead-letter a transient payload failure at the configured threshold")
        void deadLettersTransientPayloadFailureAtThreshold() {
            UUID eventId = storeEventWithMissingType();
            scheduleDelivery(eventId, 3);

            scheduler(3).retryCycle();

            assertThat(origin.published).isEmpty();
            assertThat(status(eventId)).isEqualTo(EventStatus.DEAD);
            RetryTask row = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(row.state()).isEqualTo(RetryState.DEAD);
            assertThat(row.reason()).isEqualTo(RetryReason.PAYLOAD_UNREADABLE);
            assertThat(row.lastError()).contains("EventSerializationException");
        }
    }

    @Nested
    @DisplayName("options")
    class Options {

        @Test
        @DisplayName("should compute exponential backoff capped at the maximum")
        void computesBackoff() {
            DeliveryRetryOptions options = new DeliveryRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 2.0, 0.0, Duration.ofSeconds(30), 3, 100);

            assertThat(options.backoff(1)).isEqualTo(Duration.ofSeconds(5));
            assertThat(options.backoff(2)).isEqualTo(Duration.ofSeconds(10));
            assertThat(options.backoff(3)).isEqualTo(Duration.ofSeconds(20));
            assertThat(options.backoff(4)).isEqualTo(Duration.ofSeconds(30));
        }

        @Test
        @DisplayName("should reject a blank service")
        void rejectsBlankService() {
            assertThatThrownBy(() -> new DeliveryRetryOptions(
                    "  ", Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 2.0, 0.0, Duration.ofMinutes(1), 3, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("service must not be null or blank");
        }

        @Test
        @DisplayName("should reject a non-positive interval")
        void rejectsNonPositiveInterval() {
            assertThatThrownBy(() -> new DeliveryRetryOptions(
                    SERVICE, Duration.ZERO, Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 2.0, 0.0, Duration.ofMinutes(1), 3, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("interval must be positive");
        }

        @Test
        @DisplayName("should reject a negative retry budget")
        void rejectsNegativeRetries() {
            assertThatThrownBy(() -> new DeliveryRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 2.0, 0.0, Duration.ofMinutes(1), -1, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maxRetries must be >= 0");
        }

        @Test
        @DisplayName("should reject a multiplier below one")
        void rejectsInvalidMultiplier() {
            assertThatThrownBy(() -> new DeliveryRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 0.5, 0.0, Duration.ofMinutes(1), 3, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("multiplier must be >= 1.0");
        }

        @Test
        @DisplayName("should reject jitter outside the zero-to-one range")
        void rejectsInvalidJitter() {
            assertThatThrownBy(() -> new DeliveryRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(5), 2.0, 1.5, Duration.ofMinutes(1), 3, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("jitter must be between 0 and 1");
        }

        @Test
        @DisplayName("should reject a maximum delay shorter than the base delay")
        void rejectsInvalidMaxDelay() {
            assertThatThrownBy(() -> new DeliveryRetryOptions(
                    SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(10), 2.0, 0.0, Duration.ofSeconds(5), 3, 100))
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
            DeliveryRetryScheduler scheduler = scheduler(3);
            scheduler.start();
            scheduler.stop();
        }

        @Test
        @DisplayName("should stop on close")
        void closes() {
            DeliveryRetryScheduler scheduler = scheduler(3);
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
