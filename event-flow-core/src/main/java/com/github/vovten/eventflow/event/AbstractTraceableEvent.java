package com.github.vovten.eventflow.event;

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

    private final UUID uid;
    private final UUID traceId;
    private final Instant occurredAt;

    /**
     * Creates a new traceable event with auto-generated UID, traceId and occurredAt.
     */
    protected AbstractTraceableEvent() {
        this.uid = UUID.randomUUID();
        this.traceId = UUID.randomUUID();
        this.occurredAt = Instant.now();
    }

    /**
     * Creates a new traceable event with auto-generated UID, and occurredAt.
     *
     * @param traceId the correlation ID
     */
    protected AbstractTraceableEvent(UUID traceId) {
        this.uid = UUID.randomUUID();
        this.traceId = traceId;
        this.occurredAt = Instant.now();
    }

    /**
     * Creates a traceable event with specified UID, traceId and occurredAt.
     *
     * @param uid the unique identifier
     * @param traceId the correlation ID
     * @param occurredAt the event timestamp
     */
    protected AbstractTraceableEvent(UUID uid, UUID traceId, Instant occurredAt) {
        this.uid = Objects.requireNonNull(uid, "UID must not be null");
        this.traceId = Objects.requireNonNull(traceId, "TraceId must not be null");
        this.occurredAt = Objects.requireNonNull(occurredAt, "OccurredAt must not be null");
    }

    @Override
    public UUID uid() {
        return uid;
    }

    @Override
    public UUID traceId() {
        return traceId;
    }

    @Override
    public Instant occurredAt() {
        return occurredAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractTraceableEvent that = (AbstractTraceableEvent) o;
        return uid.equals(that.uid);
    }

    @Override
    public int hashCode() {
        return uid.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s{uid=%s, traceId=%s, occurredAt=%s, type=%s}",
                getClass().getSimpleName(),
                uid,
                traceId,
                occurredAt,
                type().getSimpleName()
        );
    }
}