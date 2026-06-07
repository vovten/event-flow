package io.github.vovten.eventflow.lifecycle.store;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable record representing an event stored in the {@link EventStore}.
 * <p>
 * Contains the JSON payload, lifecycle status, optional process correlation ID,
 * and the service name that originated the event.
 *
 * @param eventId      unique event identifier
 * @param eventType    simple class name of the event type (for display and queries)
 * @param service      name of the service that originated the event, or null if unknown
 * @param payload      JSON-serialized event data
 * @param processId    optional process/correlation ID for event correlation
 * @param status       current lifecycle status
 * @param retryCount   number of retry attempts so far
 * @param createdAt    timestamp when the event was first saved
 * @param updatedAt    timestamp when the event was last updated
 * @param errorDetails error details from the last failure, or null if none
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public record StoredEvent(
        UUID eventId,
        String eventType,
        String service,
        String payload,
        UUID processId,
        EventStatus status,
        int retryCount,
        Instant createdAt,
        Instant updatedAt,
        String errorDetails
) {

    public StoredEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }

    /**
     * Creates a new event in NEW status with the given attributes.
     *
     * @param eventId   unique event identifier
     * @param eventType simple class name of the event type (for display and queries)
     * @param service   name of the originating service, or null
     * @param payload   JSON-serialized event data
     * @param processId optional process correlation ID, or null
     * @return a new StoredEvent with status NEW, retryCount 0, and timestamps set to now
     */
    public static StoredEvent newEvent(UUID eventId, String eventType, String service, String payload, UUID processId) {
        return newEvent(eventId, eventType, service, payload, processId, EventStatus.NEW);
    }

    /**
     * Creates a new event with the given status and attributes.
     *
     * @param eventId   unique event identifier
     * @param eventType simple class name of the event type (for display and queries)
     * @param service   name of the originating service, or null
     * @param payload   JSON-serialized event data
     * @param processId optional process correlation ID, or null
     * @param status    the initial lifecycle status
     * @return a new StoredEvent with the given status, retryCount 0, and timestamps set to now
     */
    public static StoredEvent newEvent(UUID eventId, String eventType, String service, String payload, UUID processId, EventStatus status) {
        Instant now = Instant.now();
        return new StoredEvent(
                eventId, eventType, service, payload, processId,
                status, 0, now, now, null
        );
    }

    /**
     * Returns a copy with the given status and error details.
     *
     * @param newStatus the new status
     * @param error     error details, or null
     * @return updated copy
     */
    public StoredEvent withStatus(EventStatus newStatus, String error) {
        return new StoredEvent(
                eventId, eventType, service, payload, processId,
                newStatus, retryCount, createdAt, Instant.now(), error
        );
    }

    /**
     * Returns a copy with incremented retry count, status reset to NEW, and updated timestamp.
     *
     * @return updated copy for retry
     */
    public StoredEvent withRetry() {
        return new StoredEvent(
                eventId, eventType, service, payload, processId,
                EventStatus.NEW, retryCount + 1, createdAt, Instant.now(), null
        );
    }

    /**
     * Checks whether the exponential backoff period has elapsed for this event,
     * meaning it is ready for another retry attempt.
     * <p>
     * The delay grows with each attempt: {@code minAge × 2^retryCount}.
     * The timer starts from the event's {@link #updatedAt()} timestamp,
     * which is refreshed after each failed or stuck retry attempt.
     *
     * @param minAge base backoff interval (delay = minAge × 2^retryCount)
     * @return {@code true} if enough time has passed since {@link #updatedAt()}
     *         to allow the next retry, {@code false} otherwise
     */
    public boolean isReadyForRetry(Duration minAge) {
        Duration backoff = minAge.multipliedBy(1L << retryCount);
        Instant retryAt = updatedAt.plus(backoff);
        return !Instant.now().isBefore(retryAt);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StoredEvent that)) return false;
        return Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }

    @Override
    public String toString() {
        return "StoredEvent{" +
                "eventId=" + eventId +
                ", eventType='" + eventType + '\'' +
                ", status=" + status +
                ", retryCount=" + retryCount +
                ", createdAt=" + createdAt +
                '}';
    }
}
