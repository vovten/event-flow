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
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("InMemoryEventStore Tests")
class InMemoryEventStoreTest {

    private InMemoryEventStore store;
    private StoredEvent event;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        store = new InMemoryEventStore();
        eventId = UUID.randomUUID();
        event = StoredEvent.newEvent(eventId, "test.TestEvent", "{\"data\":\"test\"}", null);
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
    @DisplayName("Should find events by status and age")
    void shouldFindByStatusAndAge() {
        store.save(event);
        store.updateStatus(eventId, EventStatus.FAILED, "error");

        // Should not find events updated in the future
        Instant recent = Instant.now().minus(1, ChronoUnit.MILLIS);
        List<StoredEvent> results = store.findByStatus(EventStatus.FAILED, recent);
        assertThat(results).isEmpty();

        // Should find events older than 1 second
        Instant past = Instant.now().plus(1, ChronoUnit.DAYS);
        results = store.findByStatus(EventStatus.FAILED, past);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).eventId()).isEqualTo(eventId);
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
        store.save(StoredEvent.newEvent(UUID.randomUUID(), "test.Other", "{\"data\":\"other\"}", null));
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
}
