package io.github.vovten.eventflow.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Abstract base class for traceable events providing automatic generation of unique identifiers,
 * process IDs, and timestamps.
 * <p>
 * This class implements the {@link TraceableEvent} interface and provides ready-to-use
 * fields for event tracing, correlation, and timing. It's designed to be extended by
 * concrete event classes, reducing boilerplate code and ensuring consistent event structure.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 */
public abstract class AbstractTraceableEvent implements TraceableEvent {

    private final UUID eventId;
    private final UUID processId;
    private final Instant occurredAt;

    /**
     * Creates a new traceable event with auto-generated eventId, null processId and occurredAt.
     */
    protected AbstractTraceableEvent() {
        this.eventId = UUID.randomUUID();
        this.processId = null;
        this.occurredAt = Instant.now();
    }

    /**
     * Creates a new traceable event with auto-generated eventId, specified processId, and occurredAt.
     *
     * @param processId the process identifier
     */
    protected AbstractTraceableEvent(UUID processId) {
        this.eventId = UUID.randomUUID();
        this.processId = processId;
        this.occurredAt = Instant.now();
    }

    /**
     * Creates a new traceable event with specified processId and occurredAt.
     *
     * @param processId  the process identifier
     * @param occurredAt the event timestamp
     */
    protected AbstractTraceableEvent(UUID processId, Instant occurredAt) {
        this.eventId = UUID.randomUUID();
        this.processId = processId;
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    }

    /**
     * Creates a new traceable event with specified occurredAt and no processId.
     *
     * @param occurredAt the event timestamp
     */
    protected AbstractTraceableEvent(Instant occurredAt) {
        this.eventId = UUID.randomUUID();
        this.processId = null;
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    }

    /**
     * Creates a traceable event with specified eventId, processId and occurredAt.
     *
     * @param eventId    the unique identifier
     * @param processId  the process identifier
     * @param occurredAt the event timestamp
     */
    @JsonCreator
    protected AbstractTraceableEvent(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("processId") UUID processId,
            @JsonProperty("occurredAt") Instant occurredAt) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.processId = processId;
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    }

    @JsonGetter("eventId")
    @Override
    public UUID eventId() {
        return eventId;
    }

    @JsonGetter("processId")
    @Override
    public UUID processId() {
        return processId;
    }

    @JsonGetter("occurredAt")
    @Override
    public Instant occurredAt() {
        return occurredAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractTraceableEvent that = (AbstractTraceableEvent) o;
        return eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return eventId.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s{eventId=%s, processId=%s, occurredAt=%s, type=%s}",
                getClass().getSimpleName(),
                eventId,
                processId,
                occurredAt,
                type().getSimpleName()
        );
    }
}