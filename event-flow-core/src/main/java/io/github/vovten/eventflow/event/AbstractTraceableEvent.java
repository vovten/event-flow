package io.github.vovten.eventflow.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Abstract base class for traceable events providing automatic generation of unique identifiers,
 * correlation IDs, and timestamps.
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
    private final UUID traceId;
    private final Instant occurredAt;

    /**
     * Creates a new traceable event with auto-generated eventId, traceId and occurredAt.
     */
    protected AbstractTraceableEvent() {
        this.eventId = UUID.randomUUID();
        this.traceId = UUID.randomUUID();
        this.occurredAt = Instant.now();
    }

    /**
     * Creates a new traceable event with auto-generated eventId, and occurredAt.
     *
     * @param traceId the correlation ID
     */
    protected AbstractTraceableEvent(UUID traceId) {
        this.eventId = UUID.randomUUID();
        this.traceId = traceId;
        this.occurredAt = Instant.now();
    }

    /**
     * Creates a traceable event with specified eventId, traceId and occurredAt.
     *
     * @param eventId the unique identifier
     * @param traceId the correlation ID
     * @param occurredAt the event timestamp
     */
    @JsonCreator
    protected AbstractTraceableEvent(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("traceId") UUID traceId,
            @JsonProperty("occurredAt") Instant occurredAt) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.traceId = Objects.requireNonNull(traceId, "TraceId must not be null");
        this.occurredAt = Objects.requireNonNull(occurredAt, "OccurredAt must not be null");
    }

    @JsonGetter("eventId")
    @Override
    public UUID eventId() {
        return eventId;
    }

    @JsonGetter("traceId")
    @Override
    public UUID traceId() {
        return traceId;
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
        return String.format("%s{eventId=%s, traceId=%s, occurredAt=%s, type=%s}",
                getClass().getSimpleName(),
                eventId,
                traceId,
                occurredAt,
                type().getSimpleName()
        );
    }
}