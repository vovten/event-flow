package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("EventCleanupScheduler Tests")
class EventCleanupSchedulerTest {

    private InMemoryEventStore eventStore;
    private Instant oldTimestamp;
    private Instant recentTimestamp;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        oldTimestamp = Instant.now().minusSeconds(3600); // 1 hour ago
        recentTimestamp = Instant.now();
    }

    private EventCleanupScheduler scheduler(Duration maxAge, int batchSize) {
        return new EventCleanupScheduler(
                eventStore,
                Duration.ofMinutes(1),
                maxAge,
                batchSize,
                Duration.ZERO
        );
    }

    private UUID createEvent(EventStatus status, Instant updatedAt) {
        return createEvent(status, updatedAt, null);
    }

    private UUID createEvent(EventStatus status, Instant updatedAt, String errorDetails) {
        UUID id = UUID.randomUUID();
        StoredEvent stored = new StoredEvent(
                id, "test.TestEvent", null, "{\"data\":\"test\"}", null,
                status, 0, updatedAt, updatedAt, errorDetails
        );
        eventStore.save(stored);
        return id;
    }

    @Nested
    @DisplayName("cleanupCycle")
    class CleanupCycle {

        @Test
        @DisplayName("deletes HANDLED events older than maxAge")
        void deletesOldHandledEvents() {
            UUID eventId = createEvent(EventStatus.HANDLED, oldTimestamp);

            // maxAge = 30 minutes → event (1 hour old) is eligible
            scheduler(Duration.ofMinutes(30), 100).cleanupCycle();

            assertThat(eventStore.findById(eventId)).isEmpty();
        }

        @Test
        @DisplayName("deletes UNDEFINED events older than maxAge")
        void deletesOldUndefinedEvents() {
            UUID eventId = createEvent(EventStatus.UNDEFINED, oldTimestamp);

            scheduler(Duration.ofMinutes(30), 100).cleanupCycle();

            assertThat(eventStore.findById(eventId)).isEmpty();
        }

        @Test
        @DisplayName("does not delete FAILED events")
        void doesNotDeleteFailedEvents() {
            UUID eventId = createEvent(EventStatus.FAILED, oldTimestamp, "test error");

            scheduler(Duration.ofMinutes(30), 100).cleanupCycle();

            assertThat(eventStore.findById(eventId)).isPresent();
            assertThat(eventStore.findById(eventId).orElseThrow().status())
                    .isEqualTo(EventStatus.FAILED);
        }

        @Test
        @DisplayName("does not delete events newer than maxAge")
        void doesNotDeleteRecentEvents() {
            UUID eventId = createEvent(EventStatus.HANDLED, recentTimestamp);

            // maxAge = 1 hour → event (just created) is NOT eligible
            scheduler(Duration.ofMinutes(30), 100).cleanupCycle();

            assertThat(eventStore.findById(eventId)).isPresent();
        }

        @Test
        @DisplayName("deletes both HANDLED and UNDEFINED events")
        void deletesBothHandledAndUndefined() {
            UUID id1 = createEvent(EventStatus.HANDLED, oldTimestamp);
            UUID id2 = createEvent(EventStatus.UNDEFINED, oldTimestamp);

            scheduler(Duration.ofMinutes(30), 100).cleanupCycle();

            assertThat(eventStore.findById(id1)).isEmpty();
            assertThat(eventStore.findById(id2)).isEmpty();
        }

        @Test
        @DisplayName("does nothing when there are no terminal events")
        void noEventsToClean() {
            UUID id1 = createEvent(EventStatus.PUBLISHED, oldTimestamp);
            UUID id2 = createEvent(EventStatus.FAILED, oldTimestamp, "err");
            UUID id3 = createEvent(EventStatus.NEW, oldTimestamp);

            scheduler(Duration.ofMinutes(30), 100).cleanupCycle();

            assertThat(eventStore.findById(id1)).isPresent();
            assertThat(eventStore.findById(id2)).isPresent();
            assertThat(eventStore.findById(id3)).isPresent();
        }

        @Test
        @DisplayName("handles empty store without errors")
        void emptyStore() {
            // Should not throw
            scheduler(Duration.ofMinutes(30), 100).cleanupCycle();
        }

        @Test
        @DisplayName("deletes in batches when batch size is exceeded")
        void deletesInBatches() {
            UUID[] ids = new UUID[5];
            for (int i = 0; i < 5; i++) {
                ids[i] = createEvent(EventStatus.HANDLED, oldTimestamp);
            }

            // batchSize = 2, should delete all 5 in 3 rounds
            scheduler(Duration.ofMinutes(30), 2).cleanupCycle();

            for (UUID id : ids) {
                assertThat(eventStore.findById(id)).isEmpty();
            }
        }

        @Test
        @DisplayName("handles pause between batches without errors")
        void handlesPauseBetweenBatches() {
            // Create enough events to trigger batching
            UUID[] ids = new UUID[3];
            for (int i = 0; i < 3; i++) {
                ids[i] = createEvent(EventStatus.HANDLED, oldTimestamp);
            }

            // Use a scheduler with a real pause
            EventCleanupScheduler cleanupScheduler = new EventCleanupScheduler(
                    eventStore,
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(30),
                    2,
                    Duration.ofMillis(10)
            );

            // Should not throw despite the pause
            cleanupScheduler.cleanupCycle();

            for (UUID id : ids) {
                assertThat(eventStore.findById(id)).isEmpty();
            }
        }
    }

    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("start and stop without errors")
        void startAndStop() {
            try (var cleanupScheduler = new EventCleanupScheduler(
                    eventStore,
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(30),
                    100,
                    Duration.ZERO
            )) {
                cleanupScheduler.start();
                cleanupScheduler.stop();
            }
        }

        @Test
        @DisplayName("close calls stop without errors")
        void close() {
            EventCleanupScheduler cleanupScheduler = new EventCleanupScheduler(
                    eventStore,
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(30),
                    100,
                    Duration.ZERO
            );
            cleanupScheduler.start();
            cleanupScheduler.close();
        }
    }
}
