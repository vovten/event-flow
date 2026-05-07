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
     * @param id          event ID
     * @param status      new status (PUBLISHED or FAILED)
     * @param publishedAt timestamp of successful publication (null for FAILED)
     */
    void updateStatus(UUID id, EventStatus status, Instant publishedAt);

    /**
     * Update the status of an event with error message.
     *
     * @param id           event ID
     * @param status       new status (FAILED)
     * @param errorMessage error message if status is FAILED
     */
    default void updateStatus(UUID id, EventStatus status, String errorMessage) {
        updateStatus(id, status, (Instant) null);
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
     * Find failed events for retry.
     *
     * @param limit maximum number of events to return
     * @return list of failed event records
     */
    default List<EventRecord> findFailed(int limit) {
        return List.of();
    }
}