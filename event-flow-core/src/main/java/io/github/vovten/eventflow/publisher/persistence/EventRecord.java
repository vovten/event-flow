package io.github.vovten.eventflow.publisher.persistence;

import java.time.Instant;
import java.util.UUID;

/**
 * Record representing a persisted event in the outbox table.
 * The event contains the entire serialized event as JSON.
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public class EventRecord {

    private final UUID id;
    private final UUID processId;
    private final String event; // JSON - full serialized event (Envelope or Event)
    private final Instant createdAt;
    private boolean retry;
    private String errorMessage;
    private EventStatus status = EventStatus.PENDING;

    public EventRecord(UUID id, UUID processId, String event, Instant createdAt) {
        this.id = id;
        this.processId = processId;
        this.event = event;
        this.createdAt = createdAt;
    }

    public static EventRecord create(UUID id, UUID processId, String event) {
        return new EventRecord(id, processId, event, Instant.now());
    }

    public UUID id() {
        return id;
    }

    public UUID processId() {
        return processId;
    }

    public String event() {
        return event;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public boolean retry() {
        return retry;
    }

    public EventRecord retry(boolean retry) {
        this.retry = retry;
        return this;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public EventStatus status() {
        return status;
    }

    public EventRecord status(EventStatus status) {
        this.status = status;
        return this;
    }

    public EventRecord errorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }

    @Override
    public String toString() {
        return "EventRecord{" +
                "id=" + id +
                ", processId=" + processId +
                ", status=" + status +
                ", retry=" + retry +
                ", createdAt=" + createdAt +
                '}';
    }
}