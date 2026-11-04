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
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.util.EventUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RetrySchedulerSupport Tests")
class RetrySchedulerSupportTest {

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

    @Test
    @DisplayName("should leave a delay unchanged when jitter is disabled")
    void leavesDelayWithoutJitter() {
        assertThat(RetrySchedulerSupport.withJitter(Duration.ofSeconds(10), 0.0))
                .isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("should keep a jittered delay inside the configured bounds")
    void keepsJitterInsideBounds() {
        Duration delay = Duration.ofMillis(1000);

        for (int i = 0; i < 100; i++) {
            Duration jittered = RetrySchedulerSupport.withJitter(delay, 0.5);
            assertThat(jittered).isBetween(Duration.ofMillis(500), Duration.ofMillis(1500));
        }
    }

    @Test
    @DisplayName("should publish once per cycle when both retry kinds are due")
    void publishesOnceWhenBothKindsAreDue() {
        UUID eventId = storeEvent(EventStatus.FAILED);
        scheduleDelivery(eventId);
        scheduleProcessing(eventId);

        DeliveryRetryScheduler delivery = new DeliveryRetryScheduler(
                retryStore, eventStore, lifecycleRepository, lifecyclePublisher,
                new DeliveryRetryOptions(
                        SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30), 3, 100));
        ProcessingRetryScheduler processing = new ProcessingRetryScheduler(
                retryStore, eventStore, lifecycleRepository, lifecyclePublisher,
                new ProcessingRetryOptions(
                        SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30),
                        Duration.ofSeconds(5), 2.0, 0.0, Duration.ofMinutes(1), 3, 100));

        delivery.retryCycle();
        processing.retryCycle();

        assertThat(origin.published).hasSize(1);
        assertThat(retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow().state())
                .isEqualTo(RetryState.IN_PROGRESS);
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow().state())
                .isEqualTo(RetryState.PENDING);
    }

    @Test
    @DisplayName("should publish a delivery retry when no processing retry exists")
    void publishesDeliveryWhenProcessingIsAbsent() {
        UUID eventId = storeEvent(EventStatus.FAILED);
        scheduleDelivery(eventId);

        DeliveryRetryScheduler delivery = new DeliveryRetryScheduler(
                retryStore, eventStore, lifecycleRepository, lifecyclePublisher,
                new DeliveryRetryOptions(
                        SERVICE, Duration.ofMinutes(1), Duration.ofSeconds(30), 3, 100));

        delivery.retryCycle();

        assertThat(origin.published).hasSize(1);
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

    private void scheduleDelivery(UUID eventId) {
        retryStore.schedule(RetryTask.pending(
                eventId, SERVICE, RetryKind.DELIVERY, 1,
                RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null));
    }

    private void scheduleProcessing(UUID eventId) {
        retryStore.schedule(RetryTask.pending(
                eventId, SERVICE, RetryKind.PROCESSING, 1,
                RetryReason.HANDLER_FAILED, Instant.now().minusSeconds(1), "payment"));
    }

    private static final class RecordingPublisher implements EventPublisher {

        private final List<Event> published = new ArrayList<>();

        @Override
        public CompletableFuture<SendResults> publish(Event event) {
            published.add(event);
            return CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.success("dest"))));
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
