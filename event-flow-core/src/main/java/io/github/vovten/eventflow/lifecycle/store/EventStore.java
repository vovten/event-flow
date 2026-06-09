package io.github.vovten.eventflow.lifecycle.store;

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
     * Returns the type identifier for this store implementation.
     * <p>
     * Used to select the active store via the {@code store.type} configuration property.
     * Built-in types: {@code "db"} for {@link io.github.vovten.eventflow.lifecycle.store.db.JdbcEventStore}, {@code "in-memory"}
     * for {@link InMemoryEventStore}.
     *
     * @return the store type identifier (e.g., "db", "in-memory", or custom)
     */
    String getType();

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
     * Finds events with given statuses updated before {@code before},
     * limited to {@code batchSize} results.
     * <p>
     * Implementations should push the limit down to the underlying store
     * — loading all matching events and trimming in memory defeats the purpose.
     *
     * @param statuses  statuses to search for (must not be null or empty)
     * @param before    return events updated before this time
     * @param batchSize maximum number of events to return
     * @return matching events, at most {@code batchSize}
     * @since 1.3.2
     */
    List<StoredEvent> findByStatuses(List<EventStatus> statuses, Instant before, int batchSize);

    /**
     * Finds an event by its unique identifier.
     *
     * @param eventId the event ID
     * @return the event, or empty if not found
     */
    Optional<StoredEvent> findById(UUID eventId);

    /**
     * Deletes events matching any of the given statuses that were last
     * updated before the given timestamp.
     * <p>
     * Implementations should delete in batches of up to {@code batchSize}
     * rows per batch to minimise lock contention and transaction size.
     * The method returns the total number of deleted events.
     * <p>
     * Used by the cleanup scheduler to remove old terminal events
     * (HANDLED, UNDEFINED) from the store.
     *
     * @param statuses  the statuses to delete (must not be null or empty)
     * @param before    only delete events updated before this time
     * @param batchSize maximum number of events to delete per batch
     * @return the total number of deleted events
     * @since 1.3.2
     */
    int deleteByStatuses(List<EventStatus> statuses, Instant before, int batchSize);
}
