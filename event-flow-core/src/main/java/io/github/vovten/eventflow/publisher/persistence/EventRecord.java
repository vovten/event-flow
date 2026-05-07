package io.github.vovten.eventflow.publisher.persistence;

import io.github.vovten.eventflow.event.Event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Record representing a persisted event.
 */
public class EventRecord {

    private final UUID id;
    private final String payloadType;
    private final byte[] payload;
    private final UUID processId;
    private final Instant occurredAt;
    private EventStatus status;
    private Instant publishedAt;
    private String errorMessage;

    public EventRecord(UUID id, String payloadType, byte[] payload, UUID processId, Instant occurredAt) {
        this.id = id;
        this.payloadType = payloadType;
        this.payload = payload;
        this.processId = processId;
        this.occurredAt = occurredAt;
        this.status = EventStatus.PENDING;
    }

    public UUID id() {
        return id;
    }

    public String payloadType() {
        return payloadType;
    }

    public byte[] payload() {
        return payload;
    }

    public UUID processId() {
        return processId;
    }

    public Instant occurredAt() {
        return occurredAt;
    }

    public EventStatus status() {
        return status;
    }

    public void status(EventStatus status) {
        this.status = status;
    }

    public Instant publishedAt() {
        return publishedAt;
    }

    public void publishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public void errorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventRecord that = (EventRecord) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
