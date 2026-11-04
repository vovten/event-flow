package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.InternalEventChannel;
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

@DisplayName("EventLifecyclePublisher Retry Tests")
class EventLifecyclePublisherRetryTest {

    private static final String SERVICE = "test-service";

    private InMemoryEventStore eventStore;
    private InMemoryEventRetryStore retryStore;
    private InMemoryLifecycleRepository lifecycleRepository;
    private RecordingPublisher origin;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        retryStore = new InMemoryEventRetryStore();
        lifecycleRepository = new InMemoryLifecycleRepository(eventStore, retryStore);
        origin = new RecordingPublisher();
    }

    private EventLifecyclePublisher publisher(boolean requireAcceptedAck) {
        return new EventLifecyclePublisher(origin, eventStore, SERVICE, lifecycleRepository,
                new EventLifecyclePublisherOptions(requireAcceptedAck, Duration.ofSeconds(5)));
    }

    private Envelope<TestEvent> envelope(UUID eventId) {
        return new Envelope<>(eventId, null, Instant.now(), new TestEvent("data"),
                Map.of(), List.of(InternalEventChannel.class));
    }

    private EventStatus status(UUID eventId) {
        return eventStore.findById(eventId).orElseThrow().status();
    }

    @Nested
    @DisplayName("publish failure")
    class PublishFailure {

        @Test
        @DisplayName("should create a pending delivery retry")
        void createsDeliveryRetry() {
            UUID eventId = UUID.randomUUID();
            origin.result = SendResults.of(List.of(SendResult.failure("dest", "broker down")));

            publisher(true).publish(envelope(eventId)).join();

            assertThat(status(eventId)).isEqualTo(EventStatus.FAILED);
            RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.PENDING);
            assertThat(task.reason()).isEqualTo(RetryReason.PUBLISH_FAILED);
            assertThat(task.attempt()).isEqualTo(1);
            assertThat(task.nextAttemptAt()).isAfter(Instant.now().plusSeconds(3));
        }

        @Test
        @DisplayName("should create a delivery retry when the transport future fails")
        void createsDeliveryRetryOnException() {
            UUID eventId = UUID.randomUUID();
            origin.failure = new IllegalStateException("boom");

            assertThatThrownBy(() -> publisher(true).publish(envelope(eventId)).join())
                    .hasCauseInstanceOf(IllegalStateException.class);

            RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(task.lastErrorType()).isEqualTo("java.lang.IllegalStateException");
            assertThat(task.lastError()).isEqualTo("boom");
        }

        @Test
        @DisplayName("should not create a delivery retry for a scheduler-initiated publish")
        void doesNotCreateRowForSchedulerPublish() {
            UUID eventId = UUID.randomUUID();
            eventStore.save(StoredEvent.newEvent(
                    eventId, TestEvent.class.getSimpleName(), SERVICE, "{}", null, EventStatus.FAILED));
            retryStore.schedule(RetryTask.pending(
                            eventId, SERVICE, RetryKind.DELIVERY, 2,
                            RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null)
                    .withClaim("scheduler", Instant.now().plusSeconds(30)));
            origin.result = SendResults.of(List.of(SendResult.failure("dest", "broker down")));
            Envelope<TestEvent> retryEnvelope = new Envelope<>(
                    eventId, null, Instant.now(), new TestEvent("data"),
                    Map.of(RetryMetadata.RETRY_KIND, RetryKind.DELIVERY.name(),
                            RetryMetadata.ATTEMPT, "2"),
                    List.of(InternalEventChannel.class));

            publisher(true).publish(retryEnvelope).join();

            assertThat(status(eventId)).isEqualTo(EventStatus.FAILED);
            RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.IN_PROGRESS);
            assertThat(task.claimedBy()).isEqualTo("scheduler");
        }

        @Test
        @DisplayName("should keep PUBLISHED on partial success without a delivery retry")
        void partialSuccessDoesNotRetry() {
            UUID eventId = UUID.randomUUID();
            origin.result = SendResults.of(List.of(
                    SendResult.success("dest-a"),
                    SendResult.failure("dest-b", "down")));

            publisher(true).publish(envelope(eventId)).join();

            assertThat(status(eventId)).isEqualTo(EventStatus.PUBLISHED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }
    }

    @Nested
    @DisplayName("publish success")
    class PublishSuccess {

        @Test
        @DisplayName("should wait for acceptance when require-accepted-ack is true")
        void waitsForAcceptance() {
            UUID eventId = UUID.randomUUID();

            publisher(true).publish(envelope(eventId)).join();

            assertThat(status(eventId)).isEqualTo(EventStatus.PUBLISHED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }

        @Test
        @DisplayName("should mark ACCEPTED when require-accepted-ack is false")
        void marksAccepted() {
            UUID eventId = UUID.randomUUID();

            publisher(false).publish(envelope(eventId)).join();

            assertThat(status(eventId)).isEqualTo(EventStatus.ACCEPTED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }

        @Test
        @DisplayName("should clear an existing delivery retry when acceptance is not required")
        void clearsDeliveryRetry() {
            UUID eventId = UUID.randomUUID();
            origin.result = SendResults.of(List.of(SendResult.failure("dest", "down")));
            publisher(false).publish(envelope(eventId)).join();
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isPresent();
            origin.result = SendResults.of(List.of(SendResult.success("dest")));

            publisher(false).publish(envelope(eventId)).join();

            assertThat(status(eventId)).isEqualTo(EventStatus.ACCEPTED);
            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }
    }

    private static final class RecordingPublisher implements EventPublisher {

        private final List<Event> published = new ArrayList<>();
        private SendResults result = SendResults.of(
                List.of(SendResult.success("dest")));
        private RuntimeException failure;

        @Override
        public CompletableFuture<SendResults> publish(Event event) {
            published.add(event);
            if (failure != null) {
                return CompletableFuture.failedFuture(failure);
            }
            return CompletableFuture.completedFuture(result);
        }
    }

    @io.github.vovten.eventflow.event.annotation.Event(lifecycle = EventLifecycle.MANAGED)
    private record TestEvent(String data) implements Event {

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
