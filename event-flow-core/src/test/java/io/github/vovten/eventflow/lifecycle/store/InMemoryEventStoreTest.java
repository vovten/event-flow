package io.github.vovten.eventflow.lifecycle.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("InMemoryEventStore Tests")
class InMemoryEventStoreTest {

    private InMemoryEventStore store;
    private StoredEvent event;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        store = new InMemoryEventStore();
        eventId = UUID.randomUUID();
        event = StoredEvent.newEvent(eventId, "test.TestEvent", null, "{\"data\":\"test\"}", null);
    }

    @Test
    @DisplayName("Should save and find event by ID")
    void shouldSaveAndFindById() {
        store.save(event);
        Optional<StoredEvent> found = store.findById(eventId);
        assertThat(found).isPresent();
        assertThat(found.get().eventId()).isEqualTo(eventId);
        assertThat(found.get().status()).isEqualTo(EventStatus.NEW);
    }

    @Test
    @DisplayName("Should return empty when event not found")
    void shouldReturnEmptyWhenNotFound() {
        Optional<StoredEvent> found = store.findById(UUID.randomUUID());
        assertThat(found).isEmpty();
    }

    @Test
    @DisplayName("Should throw when saving duplicate event ID")
    void shouldThrowOnDuplicateSave() {
        store.save(event);
        assertThatThrownBy(() -> store.save(event))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(eventId.toString());
    }

    @Test
    @DisplayName("Should update status to PUBLISHED")
    void shouldUpdateStatusToPublished() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.PUBLISHED, null);
        StoredEvent updated = store.findById(eventId).orElseThrow();
        assertThat(updated.status()).isEqualTo(EventStatus.PUBLISHED);
        assertThat(updated.retryCount()).isZero();
    }

    @Test
    @DisplayName("Should update status to FAILED with error details")
    void shouldUpdateStatusToFailedWithError() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.FAILED, "connection timeout");
        StoredEvent updated = store.findById(eventId).orElseThrow();
        assertThat(updated.status()).isEqualTo(EventStatus.FAILED);
        assertThat(updated.errorDetails()).isEqualTo("connection timeout");
        assertThat(updated.retryCount()).isZero();
    }

    @Test
    @DisplayName("Should increment retry count on NEW status update")
    void shouldIncrementRetryOnNewStatus() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.FAILED, "error");
        store.updateStatus(eventId, EventStatus.NEW, null);
        StoredEvent updated = store.findById(eventId).orElseThrow();
        assertThat(updated.status()).isEqualTo(EventStatus.NEW);
        assertThat(updated.retryCount()).isEqualTo(1);
        assertThat(updated.errorDetails()).isNull();
    }

    @Test
    @DisplayName("Should throw when updating non-existent event")
    void shouldThrowWhenUpdatingNonExistent() {
        assertThatThrownBy(() ->
                store.updateStatus(UUID.randomUUID(), EventStatus.PUBLISHED, null)
        ).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @DisplayName("Should mark event for retry: set retry flag, clear error, preserve status and retryCount")
    void shouldMarkForRetry() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.FAILED, "original error");
        store.markForRetry(eventId);

        StoredEvent marked = store.findById(eventId).orElseThrow();
        assertThat(marked.status()).isEqualTo(EventStatus.FAILED);
        assertThat(marked.retry()).isTrue();
        assertThat(marked.retryCount()).isZero();
        assertThat(marked.errorDetails()).isNull();
    }

    @Test
    @DisplayName("Should throw when marking non-existent event for retry")
    void shouldThrowWhenMarkingNonExistentForRetry() {
        assertThatThrownBy(() ->
                store.markForRetry(UUID.randomUUID())
        ).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @DisplayName("Should find events eligible for retry by retry flag")
    void shouldFindEligibleForRetryByFlag() {
        store.save(event);
        store.markForRetry(eventId);

        Instant deadline = Instant.now().plusSeconds(10);
        List<StoredEvent> results = store.findRetryableEvents(
                List.of(EventStatus.FAILED), deadline, 10);
        assertThat(results).hasSize(1);
        assertThat(results.getFirst().eventId()).isEqualTo(eventId);
        assertThat(results.getFirst().retry()).isTrue();
    }

    @Test
    @DisplayName("Should find events eligible for retry by status")
    void shouldFindEligibleForRetryByStatus() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.FAILED, "err");

        Instant deadline = Instant.now().plusSeconds(10);
        List<StoredEvent> results = store.findRetryableEvents(
                List.of(EventStatus.FAILED), deadline, 10);
        assertThat(results).hasSize(1);
        assertThat(results.getFirst().status()).isEqualTo(EventStatus.FAILED);
    }

    @Test
    @DisplayName("Should limit results in findRetryableEvents")
    void shouldLimitFindEligibleForRetry() {
        for (int i = 0; i < 3; i++) {
            UUID id = UUID.randomUUID();
            store.save(StoredEvent.newEvent(id, "test.A", null, "{}", null));
            store.markForRetry(id);
        }
        Instant deadline = Instant.now().plusSeconds(10);
        assertThat(store.findRetryableEvents(
                List.of(EventStatus.FAILED), deadline, 2)).hasSize(2);
    }

    @Test
    @DisplayName("Should limit results in findByStatuses with limit parameter")
    void shouldLimitFindByStatuses() {
        Instant now = Instant.now();
        UUID[] ids = new UUID[5];
        for (int i = 0; i < 5; i++) {
            ids[i] = UUID.randomUUID();
            store.save(StoredEvent.newEvent(ids[i], "test.T", null, "{}", null));
            store.updateStatus(ids[i], EventStatus.FAILED, "err");
        }

        Instant deadline = now.plusSeconds(1);
        List<StoredEvent> limited = store.findByStatuses(
                List.of(EventStatus.FAILED), deadline, 2);

        assertThat(limited).hasSize(2);
    }

    @Test
    @DisplayName("Should find events by status and age")
    void shouldFindByStatusAndAge() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.FAILED, "error");

        // Should not find events updated after the deadline
        Instant recentPast = Instant.now().minus(1, ChronoUnit.MILLIS);
        List<StoredEvent> results = store.findByStatus(EventStatus.FAILED, recentPast);
        assertThat(results).isEmpty();

        // Should find events updated before the deadline
        Instant farFuture = Instant.now().plus(1, ChronoUnit.DAYS);
        results = store.findByStatus(EventStatus.FAILED, farFuture);
        assertThat(results).hasSize(1);
        assertThat(results.getFirst().eventId()).isEqualTo(eventId);
    }

    @Test
    @DisplayName("Should handle full lifecycle: NEW → FAILED → NEW → PUBLISHED → HANDLED")
    void shouldHandleFullLifecycle() {
        store.save(event);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.NEW);

        store.updateStatus(eventId, EventStatus.FAILED, "error");
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.FAILED);

        store.updateStatus(eventId, EventStatus.NEW, null);
        StoredEvent afterRetry = store.findById(eventId).orElseThrow();
        assertThat(afterRetry.status()).isEqualTo(EventStatus.NEW);
        assertThat(afterRetry.retryCount()).isEqualTo(1);

        store.updateStatus(eventId, EventStatus.PUBLISHED, null);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.PUBLISHED);

        store.updateStatus(eventId, EventStatus.HANDLED, null);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.HANDLED);
    }

    @Test
    @DisplayName("Should track size correctly")
    void shouldTrackSize() {
        assertThat(store.size()).isZero();
        store.save(event);
        assertThat(store.size()).isEqualTo(1);
        store.save(StoredEvent.newEvent(UUID.randomUUID(), "test.Other", null, "{\"data\":\"other\"}", null));
        assertThat(store.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should clear all events")
    void shouldClear() {
        store.save(event);
        assertThat(store.size()).isEqualTo(1);
        store.clear();
        assertThat(store.size()).isZero();
        assertThat(store.findById(eventId)).isEmpty();
    }

    @Test
    @DisplayName("Should delete events by statuses before deadline")
    void shouldDeleteByStatuses() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.HANDLED, null);

        UUID eventId2 = UUID.randomUUID();
        StoredEvent event2 = StoredEvent.newEvent(eventId2, "test.TestEvent", null, "{}", null);
        store.save(event2);
        store.updateStatus(eventId2, EventStatus.UNDEFINED, null);

        UUID eventId3 = UUID.randomUUID();
        StoredEvent event3 = StoredEvent.newEvent(eventId3, "test.TestEvent", null, "{}", null);
        store.save(event3);
        store.updateStatus(eventId3, EventStatus.FAILED, "error");

        Instant deadline = Instant.now().plusSeconds(1);
        int deleted = store.deleteByStatuses(
                List.of(EventStatus.HANDLED, EventStatus.UNDEFINED), deadline, 100);

        assertThat(deleted).isEqualTo(2);
        assertThat(store.findById(eventId)).isEmpty();
        assertThat(store.findById(eventId2)).isEmpty();
        assertThat(store.findById(eventId3)).isPresent();
        assertThat(store.findById(eventId3).orElseThrow().status()).isEqualTo(EventStatus.FAILED);
    }

    @Test
    @DisplayName("Should delete nothing when no events match statuses")
    void shouldDeleteNothingWhenNoMatch() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.PUBLISHED, null);

        Instant deadline = Instant.now().plusSeconds(1);
        int deleted = store.deleteByStatuses(
                List.of(EventStatus.HANDLED, EventStatus.UNDEFINED), deadline, 100);

        assertThat(deleted).isZero();
        assertThat(store.findById(eventId)).isPresent();
    }

    @Test
    @DisplayName("Should delete nothing when deadline is in the past")
    void shouldDeleteNothingWhenDeadlineInPast() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.HANDLED, null);

        Instant deadline = Instant.now().minusSeconds(1);
        // Event was just updated, not before the deadline
        int deleted = store.deleteByStatuses(
                List.of(EventStatus.HANDLED), deadline, 100);

        assertThat(deleted).isZero();
        assertThat(store.findById(eventId)).isPresent();
    }

    @Test
    @DisplayName("Should delete only events matching given statuses")
    void shouldDeleteOnlyMatchingStatuses() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.HANDLED, null);

        UUID eventId2 = UUID.randomUUID();
        StoredEvent event2 = StoredEvent.newEvent(eventId2, "test.TestEvent", null, "{}", null);
        store.save(event2);
        store.updateStatus(eventId2, EventStatus.UNDEFINED, null);

        // Only delete HANDLED, not UNDEFINED
        Instant deadline = Instant.now().plusSeconds(1);
        int deleted = store.deleteByStatuses(List.of(EventStatus.HANDLED), deadline, 100);

        assertThat(deleted).isEqualTo(1);
        assertThat(store.findById(eventId)).isEmpty();
        assertThat(store.findById(eventId2)).isPresent();
    }
}
