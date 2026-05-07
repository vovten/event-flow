package io.github.vovten.eventflow.publisher.persistence;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;

import java.time.Instant;
import java.util.UUID;

/**
 * Repository interface for persisting events before publishing.
 * <p>
 * Implement this interface to store events in any database (JDBC, JPA, MongoDB, etc.)
 * Before publishing, events are saved with status {@link EventStatus#PENDING}.
 * After successful publishing, status is updated to {@link EventStatus#PUBLISHED}.
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public interface EventRepository {

    /**
     * Save an event to the database before publishing.
     *
     * @param envelope the event envelope to save
     * @return the saved event record
     */
    EventRecord save(EventRecord record);

    /**
     * Update the status of an event after publishing.
     *
     * @param id          the event ID
     * @param status      the new status
     * @param publishedAt timestamp when published
     */
    void updateStatus(UUID id, EventStatus status, Instant publishedAt);

    /**
     * Find pending events that haven't been published yet.
     * Used by background retry jobs.
     *
     * @param limit maximum number of events to return
     * @return list of pending event records
     */
    java.util.List<EventRecord> findPending(int limit);
}
