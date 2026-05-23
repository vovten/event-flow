package io.github.vovten.eventflow.store;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistent storage for event lifecycle tracking.
 * <p>
 * Implementations store serialized events and track their status as they
 * flow through the publish → dispatch → handle lifecycle.
 * <p>
 * The store is used on the <b>publisher side</b>:
 * <ul>
 *   <li>{@link #save(StoredEvent)} — when an event is first published</li>
 *   <li>{@link #updateStatus(UUID, EventStatus, String)} — when status changes</li>
 *   <li>{@link #findByStatus(EventStatus, Instant)} — for retry scheduling</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public interface EventStore {

    /**
     * Persists a new event in the store.
     *
     * @param event the event to save (must not be null)
     * @throws IllegalArgumentException if an event with the same ID already exists
     */
    void save(StoredEvent event);

    /**
     * Updates the status of an existing event.
     * <p>
     * If the new status is {@link EventStatus#NEW}, the retry count is
     * automatically incremented (retry reset).
     *
     * @param eventId      the event to update
     * @param status       the new status
     * @param errorDetails optional error details (null if none)
     * @throws java.util.NoSuchElementException if the event is not found
     */
    void updateStatus(UUID eventId, EventStatus status, String errorDetails);

    /**
     * Finds all events with the given status that were last updated before
     * the given timestamp.
     * <p>
     * Used by the retry scheduler to find events eligible for retry.
     *
     * @param status the status to search for
     * @param before only return events updated before this time
     * @return list of matching events (never null)
     */
    List<StoredEvent> findByStatus(EventStatus status, Instant before);

    /**
     * Finds an event by its unique identifier.
     *
     * @param eventId the event ID
     * @return the event, or empty if not found
     */
    Optional<StoredEvent> findById(UUID eventId);
}
