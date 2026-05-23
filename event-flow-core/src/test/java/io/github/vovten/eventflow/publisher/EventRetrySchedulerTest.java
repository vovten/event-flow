package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.store.EventStatus;
import io.github.vovten.eventflow.store.InMemoryEventStore;
import io.github.vovten.eventflow.store.StoredEvent;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("EventRetryScheduler Tests")
class EventRetrySchedulerTest {

    private InMemoryEventStore eventStore;
    private List<Event> publishedEvents;
    private EventPublisher publisher;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        publishedEvents = new ArrayList<>();
        publisher = event -> {
            publishedEvents.add(event);
            return CompletableFuture.completedFuture(SendResults.empty());
        };
    }

    private StoredEvent createFailedEvent(UUID id, EventStatus status, int retryCount, Instant updatedAt) {
        String payload = EventUtils.toJson(new TestEvent("data"));
        return new StoredEvent(
                id, TestEvent.class.getName(), payload, null,
                status, retryCount, updatedAt, updatedAt, "test error"
        );
    }

    private EventRetryScheduler scheduler(Duration minAge, int maxRetries) {
        return new EventRetryScheduler(
                eventStore, publisher,
                Duration.ofMinutes(1), // interval doesn't matter for direct retryCycle() calls
                minAge, maxRetries
        );
    }

    private EventRetryScheduler scheduler(int maxRetries) {
        return scheduler(Duration.ZERO, maxRetries);
    }

    @Nested
    @DisplayName("retryCycle")
    class RetryCycle {

        @Test
        @DisplayName("retries PUBLISH_FAILED events within retry limit")
        void retriesPublishFailedEvents() {
            eventId = UUID.randomUUID();
            eventStore.save(createFailedEvent(eventId, EventStatus.PUBLISH_FAILED, 0, Instant.now().minusSeconds(10)));

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.NEW);
            assertThat(stored.retryCount()).isEqualTo(1);
            assertThat(publishedEvents).hasSize(1);
        }

        @Test
        @DisplayName("retries HANDLE_FAILED events within retry limit")
        void retriesHandleFailedEvents() {
            eventId = UUID.randomUUID();
            eventStore.save(createFailedEvent(eventId, EventStatus.HANDLE_FAILED, 0, Instant.now().minusSeconds(10)));

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.NEW);
            assertThat(publishedEvents).hasSize(1);
        }

        @Test
        @DisplayName("skips events that have exceeded max retries")
        void skipsEventsExceedingMaxRetries() {
            eventId = UUID.randomUUID();
            eventStore.save(createFailedEvent(eventId, EventStatus.PUBLISH_FAILED, 3, Instant.now().minusSeconds(10)));

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISH_FAILED);
            assertThat(stored.retryCount()).isEqualTo(3);
            assertThat(publishedEvents).isEmpty();
        }

        @Test
        @DisplayName("skips events below the minimum age threshold")
        void skipsEventsBelowMinAge() {
            eventId = UUID.randomUUID();
            // Event updated just now — newer than minAge of 10 seconds
            eventStore.save(createFailedEvent(eventId, EventStatus.PUBLISH_FAILED, 0, Instant.now()));

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISH_FAILED);
            assertThat(publishedEvents).isEmpty();
        }

        @Test
        @DisplayName("does nothing when there are no failed events")
        void noFailedEvents() {
            eventId = UUID.randomUUID();
            // Save event with NEW status — not failed
            String payload = EventUtils.toJson(new TestEvent("data"));
            StoredEvent newEvent = StoredEvent.newEvent(eventId, TestEvent.class.getName(), payload, null);
            eventStore.save(newEvent);

            scheduler(3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.NEW);
            assertThat(publishedEvents).isEmpty();
        }

        @Test
        @DisplayName("handles invalid JSON payload without crashing the cycle")
        void invalidJsonPayload() {
            eventId = UUID.randomUUID();
            StoredEvent invalid = new StoredEvent(
                    eventId, TestEvent.class.getName(), "{invalid-json", null,
                    EventStatus.PUBLISH_FAILED, 0, Instant.now().minusSeconds(10),
                    Instant.now().minusSeconds(10), "error"
            );
            eventStore.save(invalid);

            // Should not throw
            scheduler(3).retryCycle();

            assertThat(publishedEvents).isEmpty();
        }

        @Test
        @DisplayName("retries both PUBLISH_FAILED and HANDLE_FAILED events in one cycle")
        void retriesBothStatuses() {
            UUID id1 = UUID.randomUUID();
            UUID id2 = UUID.randomUUID();
            eventStore.save(createFailedEvent(id1, EventStatus.PUBLISH_FAILED, 0, Instant.now().minusSeconds(10)));
            eventStore.save(createFailedEvent(id2, EventStatus.HANDLE_FAILED, 0, Instant.now().minusSeconds(10)));

            scheduler(3).retryCycle();

            assertThat(eventStore.findById(id1)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.NEW));
            assertThat(eventStore.findById(id2)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.NEW));
            assertThat(publishedEvents).hasSize(2);
        }

        @Test
        @DisplayName("respects exponential backoff — skips event when backoff not elapsed")
        void skipsEventBeforeBackoffElapsed() {
            eventId = UUID.randomUUID();
            // retryCount=1 → backoff = minAge * 2^1 = 20s
            // updatedAt was 15s ago → retryAt is 5s in the future → skip
            eventStore.save(createFailedEvent(eventId, EventStatus.PUBLISH_FAILED, 1, Instant.now().minusSeconds(15)));

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISH_FAILED);
            assertThat(publishedEvents).isEmpty();
        }

        @Test
        @DisplayName("respects exponential backoff — retries event after backoff elapsed")
        void retriesEventAfterBackoffElapsed() {
            eventId = UUID.randomUUID();
            // retryCount=1 → backoff = minAge * 2^1 = 20s
            // updatedAt was 30s ago → retryAt is 10s in the past → retry
            eventStore.save(createFailedEvent(eventId, EventStatus.PUBLISH_FAILED, 1, Instant.now().minusSeconds(30)));

            scheduler(Duration.ofSeconds(10), 3).retryCycle();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.NEW);
            assertThat(stored.retryCount()).isEqualTo(2);
            assertThat(publishedEvents).hasSize(1);
        }

        @Test
        @DisplayName("exponential backoff doubles with each retry attempt")
        void backoffDoublesWithEachRetry() {
            UUID id0 = UUID.randomUUID();
            UUID id1 = UUID.randomUUID();
            UUID id2 = UUID.randomUUID();
            // retryCount=0 → backoff = 10s * 1 = 10s, updatedAt 20s ago → elapsed
            // retryCount=1 → backoff = 10s * 2 = 20s, updatedAt 20s ago → just elapsed
            // retryCount=2 → backoff = 10s * 4 = 40s, updatedAt 30s ago → NOT elapsed
            eventStore.save(createFailedEvent(id0, EventStatus.PUBLISH_FAILED, 0, Instant.now().minusSeconds(20)));
            eventStore.save(createFailedEvent(id1, EventStatus.PUBLISH_FAILED, 1, Instant.now().minusSeconds(20)));
            eventStore.save(createFailedEvent(id2, EventStatus.PUBLISH_FAILED, 2, Instant.now().minusSeconds(30)));

            scheduler(Duration.ofSeconds(10), 5).retryCycle();

            assertThat(eventStore.findById(id0)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.NEW));
            assertThat(eventStore.findById(id1)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.NEW));
            assertThat(eventStore.findById(id2)).hasValueSatisfying(e ->
                    assertThat(e.status()).isEqualTo(EventStatus.PUBLISH_FAILED));
            assertThat(publishedEvents).hasSize(2);
        }
    }

    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("start and stop without errors")
        void startAndStop() throws Exception {
            EventRetryScheduler scheduler = new EventRetryScheduler(
                    eventStore, publisher,
                    Duration.ofMinutes(1), Duration.ZERO, 3
            );
            scheduler.start();
            // Give it a moment to schedule the task
            Thread.sleep(50);
            scheduler.stop();
            // Should not throw
        }

        @Test
        @DisplayName("close calls stop without errors")
        void close() {
            EventRetryScheduler scheduler = new EventRetryScheduler(
                    eventStore, publisher,
                    Duration.ofMinutes(1), Duration.ZERO, 3
            );
            scheduler.start();
            scheduler.close();
            // Should not throw
        }
    }

    /**
     * Test event record for serialization/deserialization in retry tests.
     * Records are natively supported by Jackson 2.12+ for both serialization
     * and deserialization.
     */
    private record TestEvent(String data) implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
