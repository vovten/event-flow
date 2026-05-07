package io.github.vovten.eventflow.publisher.persistence;

import java.time.Instant;
import java.util.UUID;

/**
 * Record representing a persisted event in the outbox table.
 * The payload contains the entire serialized event as JSON.
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public class EventRecord {

    private final UUID id;
    private final String payload; // JSON
    private final Instant createdAt;
    private Instant publishedAt;
    private String errorMessage;
    private EventStatus status = EventStatus.PENDING;

    public EventRecord(UUID id, String payload, Instant createdAt) {
        this.id = id;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public static EventRecord create(UUID id, String payload) {
        return new EventRecord(id, payload, Instant.now());
    }

    public UUID id() {
        return id;
    }

    public String payload() {
        return payload;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant publishedAt() {
        return publishedAt;
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

    public EventRecord publishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
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
                ", status=" + status +
                ", createdAt=" + createdAt +
                ", publishedAt=" + publishedAt +
                '}';
    }
}