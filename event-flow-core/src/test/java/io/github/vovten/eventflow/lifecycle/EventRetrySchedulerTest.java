package io.github.vovten.eventflow.lifecycle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("EventRetryScheduler Tests")
class EventRetrySchedulerTest {

    private static final String SERVICE = "test-service";

    private InMemoryEventStore eventStore;
    private EventPublisher lifecyclePublisher;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        EventPublisher originPublisher = event ->
                CompletableFuture.completedFuture(
                        SendResults.of(List.of(SendResult.success("dest"))));
        lifecyclePublisher = new EventLifecyclePublisher(
                originPublisher, eventStore, SERVICE);
    }

    private EventRetryScheduler scheduler(Duration minAge, int maxRetries, int batchSize) {
        return scheduler(minAge, maxRetries, batchSize, SERVICE);
    }

    private EventRetryScheduler scheduler(Duration minAge, int maxRetries) {
        return scheduler(minAge, maxRetries, 1000);
    }

    private EventRetryScheduler scheduler(int maxRetries) {
        return scheduler(Duration.ZERO, maxRetries, 1000);
    }

    private EventRetryScheduler scheduler(Duration minAge, int maxRetries, int batchSize, String service) {
        return new EventRetryScheduler(
                eventStore, lifecyclePublisher,
                Duration.ofMinutes(1),
                minAge, maxRetries, batchSize, service
        );
    }

    /**
     * Creates a StoredEvent with FAILED status.
     * The eventId from the serialized payload matches the stored event ID
     * so EventLifecyclePublisher can find it on retry.
     * Uses a past timestamp (default: 10 seconds ago) so the event is
     * immediately eligible for retry with minAge=Duration.ZERO.
     */
    private UUID createFailedEvent(int retryCount) {
        return createFailedEvent(retryCount, Instant.now().minusSeconds(10));
    }

    private UUID createFailedEvent(int retryCount, Instant updatedAt) {
        return createEvent(EventStatus.FAILED, retryCount, updatedAt, "test error");
    }

    private UUID createNewEvent(int retryCount) {
        return createNewEvent(retryCount, Instant.now().minusSeconds(10));
    }

    private UUID createNewEvent(int retryCount, Instant updatedAt) {
        return createEvent(EventStatus.NEW, retryCount, updatedAt, null);
    }

    private UUID createPublishedEvent(int retryCount) {
        return createPublishedEvent(retryCount, Instant.now().minusSeconds(10));
    }

    private UUID createPublishedEvent(int retryCount, Instant updatedAt) {
        return createEvent(EventStatus.PUBLISHED, retryCount, updatedAt, null);
    }

    private UUID createEvent(EventStatus status, int retryCount, Instant updatedAt, String errorDetails) {
        return createEvent(status, retryCount, updatedAt, errorDetails, false);
    }

    private UUID createEvent(EventStatus status, int retryCount, Instant updatedAt, String errorDetails, boolean retryFlag) {
        UUID id = UUID.randomUUID();
        TestEvent event = new TestEvent(id, "data");
        String payload = EventUtils.toJson(event);
        StoredEvent stored = new StoredEvent(
                id, TestEvent.class.getName(), SERVICE, payload, null, null,
                status, retryCount, retryFlag, updatedAt, updatedAt, errorDetails
        );
        eventStore.save(stored);
        return id;
    }

    private UUID createRetryFlaggedEvent(EventStatus status, int retryCount, Instant updatedAt) {
        return createEvent(status, retryCount, updatedAt, null, true);
    }

    /**
     * Creates a FAILED event attributed to the given service, updated 10 seconds ago
     * so it is immediately eligible for retry with minAge=Duration.ZERO.
     */
    private UUID createFailedEventForService(String service, int retryCount) {
        UUID id = UUID.randomUUID();
        TestEvent event = new TestEvent(id, "data");
        String payload = EventUtils.toJson(event);
        StoredEvent stored = new StoredEvent(
                id, TestEvent.class.getName(), service, payload, null, null,
                EventStatus.FAILED, retryCount, false,
                Instant.now().minusSeconds(10), Instant.now().minusSeconds(10), "test error"
        );
        eventStore.save(stored);
        return id;
    }

    /**
     * Stores a FAILED event whose payload is an Envelope with explicit channels,
     * using the given channels column value (may be null or blank).
     */
    private void storeFailedEnvelope(UUID eventId, String channels) {
        Envelope<?> envelope = new Envelope<>(
                eventId, null, Instant.now(), new TestEvent(eventId, "data"),
                Map.of(), List.of(ExternalEventChannel.class));
        eventStore.save(new StoredEvent(
                eventId, TestEvent.class.getName(), SERVICE, EventUtils.toJson(envelope),
                channels, null,
                EventStatus.FAILED, 0, false,
                Instant.now().minusSeconds(10), Instant.now().minusSeconds(10), "test error"));
    }

    @Nested
    @DisplayName("retryCycle")
    class RetryCycle {

        @Test
        @DisplayName("retries FAILED events within retry limit")
        void retriesFailedEvents() {
            UUID eventId = createFailedEvent(0);

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retryCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("skips events that have exceeded max retries")
        void skipsEventsExceedingMaxRetries() {
            // Create event with retryCount = maxRetries → should NOT be retried
            UUID eventId = createFailedEvent(3, Instant.now().minusSeconds(10));

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.FAILED);
            assertThat(stored.retryCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("skips events below the minimum age threshold")
        void skipsEventsBelowMinAge() {
            // Event updated just now — newer than minAge of 10 seconds
            UUID eventId = createFailedEvent(0, Instant.now());

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.FAILED);
        }

        @Test
        @DisplayName("does nothing when there are no failed events")
        void noFailedEvents() {
            // Publish an event successfully — status will be PUBLISHED, not FAILED
            TestEvent event = new TestEvent("data");
            lifecyclePublisher.publish(event).join();

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(event.eventId()).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
        }

        @Test
        @DisplayName("handles invalid JSON payload without crashing the cycle")
        void invalidJsonPayload() {
            UUID eventId = UUID.randomUUID();
            StoredEvent invalid = new StoredEvent(
                    eventId, TestEvent.class.getName(), SERVICE, "{invalid-json", null, null,
                    EventStatus.FAILED, 0, false, Instant.now().minusSeconds(10),
                    Instant.now().minusSeconds(10), "error"
            );
            eventStore.save(invalid);

            // Should not throw
            scheduler(3).retryCycle();
        }

        @Test
        @DisplayName("restores explicit channels from the store when retrying an envelope")
        void restoresChannelsFromStoreOnRetry() {
            AtomicReference<Event> captured = new AtomicReference<>();
            EventPublisher capturingOrigin = e -> {
                captured.set(e);
                return CompletableFuture.completedFuture(
                        SendResults.of(List.of(SendResult.success("dest"))));
            };
            EventLifecyclePublisher capturingLifecycle =
                    new EventLifecyclePublisher(capturingOrigin, eventStore, SERVICE);
            EventRetryScheduler scheduler = new EventRetryScheduler(
                    eventStore, capturingLifecycle,
                    Duration.ofMinutes(1), Duration.ZERO, 3, 1000, SERVICE);

            UUID eventId = UUID.randomUUID();
            Envelope<?> envelope = new Envelope<>(
                    eventId, null, Instant.now(), new TestEvent(eventId, "data"),
                    Map.of(), List.of(ExternalEventChannel.class));
            String payload = EventUtils.toJson(envelope);
            StoredEvent stored = new StoredEvent(
                    eventId, TestEvent.class.getName(), SERVICE, payload,
                    ExternalEventChannel.class.getName(), null,
                    EventStatus.FAILED, 0, false,
                    Instant.now().minusSeconds(10), Instant.now().minusSeconds(10), "test error"
            );
            eventStore.save(stored);

            scheduler.retryCycle();

            assertThat(captured.get()).isNotNull();
            assertThat(captured.get().channels()).containsExactly(ExternalEventChannel.class);
            StoredEvent updated = eventStore.findById(eventId).orElseThrow();
            assertThat(updated.status()).isEqualTo(EventStatus.PUBLISHED);
        }

        @Test
        @DisplayName("falls back to annotation-based channels when stored channel names are unresolvable")
        void fallsBackToAnnotationChannelsWhenStoredNamesUnresolvable() {
            AtomicReference<Event> captured = new AtomicReference<>();
            EventPublisher capturingOrigin = e -> {
                captured.set(e);
                return CompletableFuture.completedFuture(
                        SendResults.of(List.of(SendResult.success("dest"))));
            };
            EventLifecyclePublisher capturingLifecycle =
                    new EventLifecyclePublisher(capturingOrigin, eventStore, SERVICE);
            EventRetryScheduler scheduler = new EventRetryScheduler(
                    eventStore, capturingLifecycle,
                    Duration.ofMinutes(1), Duration.ZERO, 3, 1000, SERVICE);

            UUID eventId = UUID.randomUUID();
            Envelope<?> envelope = new Envelope<>(
                    eventId, null, Instant.now(), new TestEvent(eventId, "data"),
                    Map.of(), List.of(ExternalEventChannel.class));
            StoredEvent stored = new StoredEvent(
                    eventId, TestEvent.class.getName(), SERVICE, EventUtils.toJson(envelope),
                    "com.example.UnknownChannel,java.lang.String", null,
                    EventStatus.FAILED, 0, false,
                    Instant.now().minusSeconds(10), Instant.now().minusSeconds(10), "test error"
            );
            eventStore.save(stored);

            scheduler.retryCycle();

            assertThat(captured.get()).isNotNull();
            assertThat(captured.get().channels()).containsExactly(InternalEventChannel.class);
            StoredEvent updated = eventStore.findById(eventId).orElseThrow();
            assertThat(updated.status()).isEqualTo(EventStatus.PUBLISHED);
        }

        @Test
        @DisplayName("keeps the deserialized envelope when stored channels are null or blank")
        void keepsOriginalEventWhenStoredChannelsAreNullOrBlank() {
            List<Event> captured = new ArrayList<>();
            EventPublisher capturingOrigin = e -> {
                captured.add(e);
                return CompletableFuture.completedFuture(
                        SendResults.of(List.of(SendResult.success("dest"))));
            };
            EventLifecyclePublisher capturingLifecycle =
                    new EventLifecyclePublisher(capturingOrigin, eventStore, SERVICE);
            EventRetryScheduler scheduler = new EventRetryScheduler(
                    eventStore, capturingLifecycle,
                    Duration.ofMinutes(1), Duration.ZERO, 3, 1000, SERVICE);

            UUID nullChannelsId = UUID.randomUUID();
            storeFailedEnvelope(nullChannelsId, null);
            UUID blankChannelsId = UUID.randomUUID();
            storeFailedEnvelope(blankChannelsId, "   ");

            scheduler.retryCycle();

            assertThat(captured).hasSize(2);
            assertThat(captured).allSatisfy(event ->
                    assertThat(event.channels()).containsExactly(InternalEventChannel.class));
            assertThat(eventStore.findById(nullChannelsId)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
            assertThat(eventStore.findById(blankChannelsId)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
        }

        @Test
        @DisplayName("restores resolvable channel names and skips unresolvable ones")
        void restoresResolvableChannelsSkippingUnresolvableOnes() {
            AtomicReference<Event> captured = new AtomicReference<>();
            EventPublisher capturingOrigin = e -> {
                captured.set(e);
                return CompletableFuture.completedFuture(
                        SendResults.of(List.of(SendResult.success("dest"))));
            };
            EventLifecyclePublisher capturingLifecycle =
                    new EventLifecyclePublisher(capturingOrigin, eventStore, SERVICE);
            EventRetryScheduler scheduler = new EventRetryScheduler(
                    eventStore, capturingLifecycle,
                    Duration.ofMinutes(1), Duration.ZERO, 3, 1000, SERVICE);

            UUID eventId = UUID.randomUUID();
            Envelope<?> envelope = new Envelope<>(
                    eventId, null, Instant.now(), new TestEvent(eventId, "data"),
                    Map.of(), List.of(ExternalEventChannel.class));
            StoredEvent stored = new StoredEvent(
                    eventId, TestEvent.class.getName(), SERVICE, EventUtils.toJson(envelope),
                    "com.example.UnknownChannel," + ExternalEventChannel.class.getName(), null,
                    EventStatus.FAILED, 0, false,
                    Instant.now().minusSeconds(10), Instant.now().minusSeconds(10), "test error");
            eventStore.save(stored);

            scheduler.retryCycle();

            assertThat(captured.get()).isNotNull();
            assertThat(captured.get().channels()).containsExactly(ExternalEventChannel.class);
            StoredEvent updated = eventStore.findById(eventId).orElseThrow();
            assertThat(updated.status()).isEqualTo(EventStatus.PUBLISHED);
        }

        @Test
        @DisplayName("retries multiple FAILED events in one cycle")
        void retriesMultipleFailed() {
            UUID id1 = createFailedEvent(0);
            UUID id2 = createFailedEvent(0);

            scheduler(3).retryCycle();

            assertThat(eventStore.findById(id1)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
            assertThat(eventStore.findById(id2)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
        }

        @Test
        @DisplayName("retries only events published by its own service")
        void retriesOnlyOwnServiceEvents() {
            UUID ownId = createFailedEventForService("svc-a", 0);
            UUID foreignId = createFailedEventForService("svc-b", 0);

            scheduler(Duration.ZERO, 3, 1000, "svc-a").retryCycle();

            StoredEvent own = eventStore.findById(ownId).orElseThrow();
            assertThat(own.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(own.retryCount()).isEqualTo(1);

            StoredEvent foreign = eventStore.findById(foreignId).orElseThrow();
            assertThat(foreign.status()).isEqualTo(EventStatus.FAILED);
            assertThat(foreign.retryCount()).isZero();
        }

        @Test
        @DisplayName("respects batchSize — retries at most N events per cycle")
        void respectsBatchSize() {
            UUID[] ids = new UUID[5];
            for (int i = 0; i < 5; i++) {
                ids[i] = createFailedEvent(0);
            }

            // batchSize = 2, should retry only 2 out of 5 events
            scheduler(Duration.ZERO, 5, 2).retryCycle();

            int retried = 0;
            int skipped = 0;
            for (UUID id : ids) {
                StoredEvent stored = eventStore.findById(id).orElseThrow();
                if (stored.status() == EventStatus.PUBLISHED) {
                    retried++;
                } else {
                    skipped++;
                }
            }
            assertThat(retried).isEqualTo(2);
            assertThat(skipped).isEqualTo(3);
        }

        @Test
        @DisplayName("respects exponential backoff — skips event when backoff not elapsed")
        void skipsEventBeforeBackoffElapsed() {
            // retryCount=1 → backoff = minAge * 2^1 = 20s
            // updatedAt was 15s ago → retryAt is 5s in the future → skip
            UUID eventId = createFailedEvent(1, Instant.now().minusSeconds(15));

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.FAILED);
        }

        @Test
        @DisplayName("respects exponential backoff — retries event after backoff elapsed")
        void retriesEventAfterBackoffElapsed() {
            // retryCount=1 → backoff = minAge * 2^1 = 20s
            // updatedAt was 30s ago → retryAt is 10s in the past → retry
            UUID eventId = createFailedEvent(1, Instant.now().minusSeconds(30));

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retryCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("exponential backoff doubles with each retry attempt")
        void backoffDoublesWithEachRetry() {
            // retryCount=0 → backoff = 10s * 1 = 10s, updatedAt 20s ago → elapsed
            // retryCount=1 → backoff = 10s * 2 = 20s, updatedAt 20s ago → just elapsed
            // retryCount=2 → backoff = 10s * 4 = 40s, updatedAt 30s ago → NOT elapsed
            UUID id0 = createFailedEvent(0, Instant.now().minusSeconds(20));
            UUID id1 = createFailedEvent(1, Instant.now().minusSeconds(20));
            UUID id2 = createFailedEvent(2, Instant.now().minusSeconds(30));

            scheduler(Duration.ofSeconds(10), 5).retryCycle();

            assertThat(eventStore.findById(id0)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
            assertThat(eventStore.findById(id1)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
            assertThat(eventStore.findById(id2)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.FAILED));
        }

        @Test
        @DisplayName("retries PUBLISHED events within retry limit")
        void retriesPublishedEvents() {
            UUID eventId = createPublishedEvent(0);

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retryCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("skips PUBLISHED events that have exceeded max retries")
        void skipsPublishedEventsExceedingMaxRetries() {
            UUID eventId = createPublishedEvent(3, Instant.now().minusSeconds(10));

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retryCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("skips recent PUBLISHED events below minimum age threshold")
        void skipsRecentPublishedEvents() {
            UUID eventId = createPublishedEvent(0, Instant.now());

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retryCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("retries mix of FAILED and PUBLISHED events in one cycle")
        void retriesMixOfFailedAndPublished() {
            UUID failedId = createFailedEvent(0);
            UUID stuckId = createPublishedEvent(0);

            scheduler(3).retryCycle();

            assertThat(eventStore.findById(failedId)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
            assertThat(eventStore.findById(stuckId)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
            assertThat(eventStore.findById(failedId)).hasValueSatisfying(e ->
                    assertThat(e.retryCount()).isEqualTo(1));
            assertThat(eventStore.findById(stuckId)).hasValueSatisfying(e ->
                    assertThat(e.retryCount()).isEqualTo(1));
        }

        @Test
        @DisplayName("respects exponential backoff for PUBLISHED events")
        void skipsPublishedEventBeforeBackoffElapsed() {
            UUID eventId = createPublishedEvent(1, Instant.now().minusSeconds(15));

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
        }

        // -------------------------------------------------------
        // NEW events (stuck after crash before publish)
        // -------------------------------------------------------

        @Test
        @DisplayName("retries NEW events within retry limit")
        void retriesNewEvents() {
            UUID eventId = createNewEvent(0);

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retryCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("skips NEW events that have exceeded max retries")
        void skipsNewEventsExceedingMaxRetries() {
            UUID eventId = createNewEvent(3, Instant.now().minusSeconds(10));

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.NEW);
            assertThat(stored.retryCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("skips recent NEW events below minimum age threshold")
        void skipsRecentNewEvents() {
            UUID eventId = createNewEvent(0, Instant.now());

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.NEW);
            assertThat(stored.retryCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("retries mix of FAILED, PUBLISHED and NEW events in one cycle")
        void retriesMixOfFailedPublishedAndNew() {
            UUID failedId = createFailedEvent(0);
            UUID stuckId = createPublishedEvent(0);
            UUID newId = createNewEvent(0);

            scheduler(3).retryCycle();

            assertThat(eventStore.findById(failedId)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
            assertThat(eventStore.findById(stuckId)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
            assertThat(eventStore.findById(newId)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISHED));
        }

        @Test
        @DisplayName("preserves retry increment across multiple retry cycles")
        void preservesRetryIncrementAcrossCycles() {
            UUID eventId = createFailedEvent(0);

            // First retry cycle
            scheduler(5).retryCycle();
            StoredEvent afterFirstRetry = eventStore.findById(eventId).orElseThrow();
            assertThat(afterFirstRetry.retryCount()).isEqualTo(1);
            assertThat(afterFirstRetry.status()).isEqualTo(EventStatus.PUBLISHED);

            // Set back to FAILED for another retry
            eventStore.updateStatus(eventId, EventStatus.FAILED, "error again");

            // Second retry cycle
            scheduler(5).retryCycle();
            StoredEvent afterSecondRetry = eventStore.findById(eventId).orElseThrow();
            assertThat(afterSecondRetry.retryCount()).isEqualTo(2);
            assertThat(afterSecondRetry.status()).isEqualTo(EventStatus.PUBLISHED);
        }

        // -------------------------------------------------------
        // Manual retry (retry flag)
        // -------------------------------------------------------

        @Test
        @DisplayName("retries events with retry flag set, regardless of status")
        void retriesRetryFlaggedEvents() {
            UUID eventId = createRetryFlaggedEvent(EventStatus.UNDEFINED, 0, Instant.now().minusSeconds(10));

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            // TestEvent has MANAGED lifecycle → publish sets it to PUBLISHED
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retry()).isFalse();
        }

        @Test
        @DisplayName("retries event with retry flag even when maxRetries exceeded")
        void retriesRetryFlaggedEventBeyondMaxRetries() {
            UUID eventId = createRetryFlaggedEvent(EventStatus.FAILED, 5, Instant.now().minusSeconds(10));

            // maxRetries=3 — event has retryCount=5, but manual retry bypasses the limit
            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retry()).isFalse();
            // manual retries do not consume the automatic retry budget
            assertThat(stored.retryCount()).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("constructor")
    class Constructor {

        @Test
        @DisplayName("rejects null service")
        void rejectsNullService() {
            assertThatThrownBy(() -> new EventRetryScheduler(
                    eventStore, lifecyclePublisher,
                    Duration.ofMinutes(1), Duration.ZERO, 3, 1000, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("service must not be null or blank");
        }

        @Test
        @DisplayName("rejects blank service")
        void rejectsBlankService() {
            assertThatThrownBy(() -> new EventRetryScheduler(
                    eventStore, lifecyclePublisher,
                    Duration.ofMinutes(1), Duration.ZERO, 3, 1000, "  "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("service must not be null or blank");
        }
    }

    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("start and stop without errors")
        void startAndStop() {
            try (var scheduler = new EventRetryScheduler(
                    eventStore, lifecyclePublisher,
                    Duration.ofMinutes(1), Duration.ZERO, 3, 1000, SERVICE
            )) {
                scheduler.start();
                scheduler.stop();
            }
        }

        @Test
        @DisplayName("close calls stop without errors")
        void close() {
            EventRetryScheduler scheduler = new EventRetryScheduler(
                    eventStore, lifecyclePublisher,
                    Duration.ofMinutes(1), Duration.ZERO, 3, 1000, SERVICE
            );
            scheduler.start();
            scheduler.close();
        }
    }

    /**
     * Test event with MANAGED lifecycle for proper retry integration.
     * Extends AbstractTraceableEvent so eventId is preserved through JSON
     * serialization/deserialization when EventLifecyclePublisher resolves it.
     * Uses a {@code @JsonCreator} on the all-args constructor for proper
     * deserialization by the retry scheduler.
     */
    @io.github.vovten.eventflow.event.annotation.Event(lifecycle = EventLifecycle.MANAGED)
    private static class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
            this.data = data;
        }

        /** Constructor with explicit eventId for matching stored event IDs. */
        @JsonCreator
        TestEvent(@JsonProperty("eventId") UUID eventId,
                  @JsonProperty("data") String data) {
            super(eventId, null, java.time.Instant.now());
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @SuppressWarnings("unused")
        public String getData() {
            return data;
        }
    }
}
