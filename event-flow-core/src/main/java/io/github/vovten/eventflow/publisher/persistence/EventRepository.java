package io.github.vovten.eventflow.publisher.persistence;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository interface for persisting events to database.
 * <p>
 * Implementations handle different databases (PostgreSQL, MySQL, etc.)
 * with support for the transactional outbox pattern.
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public interface EventRepository {

    /**
     * Save an event record to the database.
     *
     * @param record event record with all data
     */
    void save(EventRecord record);

    /**
     * Update the status of an event.
     *
     * @param id     event ID
     * @param status new status (PUBLISHED or FAILED)
     */
    void updateStatus(UUID id, EventStatus status);

    /**
     * Update the status of an event with error message.
     *
     * @param id           event ID
     * @param status       new status (FAILED)
     * @param errorMessage error message if status is FAILED
     */
    void updateStatus(UUID id, EventStatus status, String errorMessage);

    /**
     * Update event for retry.
     * Sets retry flag and updates modifiedAt.
     *
     * @param id event ID
     */
    default void markForRetry(UUID id) {
        updateFields(id, new FieldUpdate().retry(true).modifiedAt(Instant.now()));
    }

    /**
     * Update event after failed publish attempt.
     * Increments retryCount, updates modifiedAt and errorMessage.
     *
     * @param id           event ID
     * @param errorMessage error message from publish attempt
     * @return new retry count
     */
    int markFailed(UUID id, String errorMessage);

    /**
     * Field update helper for flexible updates.
     */
    class FieldUpdate {
        private Boolean retry;
        private Instant modifiedAt;
        private EventStatus status;

        public FieldUpdate retry(boolean retry) {
            this.retry = retry;
            return this;
        }

        public FieldUpdate modifiedAt(Instant modifiedAt) {
            this.modifiedAt = modifiedAt;
            return this;
        }

        public FieldUpdate status(EventStatus status) {
            this.status = status;
            return this;
        }

        public Boolean retry() {
            return retry;
        }

        public Instant modifiedAt() {
            return modifiedAt;
        }

        public EventStatus status() {
            return status;
        }
    }

    /**
     * Update multiple fields of an event.
     *
     * @param id     event ID
     * @param update field updates to apply
     */
    default void updateFields(UUID id, FieldUpdate update) {
        // Default no-op - override in implementation
    }

    /**
     * Find an event by ID.
     *
     * @param id event ID
     * @return event record or empty
     */
    default Optional<EventRecord> findById(UUID id) {
        return Optional.empty();
    }

    /**
     * Find pending events that need to be published.
     *
     * @param limit maximum number of events to return
     * @return list of pending event records
     */
    List<EventRecord> findPending(int limit);

    /**
     * Find failed events for manual retry (retry=true).
     *
     * @param limit maximum number of events to return
     * @return list of failed event records
     */
    default List<EventRecord> findFailed(int limit) {
        return List.of();
    }

    /**
     * Find failed events eligible for automatic retry.
     * Should return events where:
     * - status = FAILED
     * - retryCount < maxRetryCount
     * - modifiedAt + delay <= now()
     *
     * @param limit           maximum number of events to return
     * @param maxRetryCount   maximum retry count for events
     * @param minModifiedAt   events modified before this time are eligible (for delay calculation)
     * @return list of failed event records ready for retry
     */
    List<EventRecord> findFailedForRetry(int limit, int maxRetryCount, Instant minModifiedAt);
}