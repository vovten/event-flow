package com.github.vovten.eventflow.test;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventBus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Test event for external bus (Kafka)
 */
public class ExternalTestEvent implements Event {

    private String id;
    private String payload;
    private LocalDateTime timestamp;

    public ExternalTestEvent() {
        this(UUID.randomUUID().toString(), "External test event payload", LocalDateTime.now());
    }

    public ExternalTestEvent(String id, String payload, LocalDateTime timestamp) {
        this.id = id;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public static ExternalTestEvent create() {
        return new ExternalTestEvent();
    }

    public static ExternalTestEvent create(String payload) {
        return new ExternalTestEvent(UUID.randomUUID().toString(), payload, LocalDateTime.now());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Alias for payload to maintain consistency with TestEvent
     */
    public String getMessage() {
        return payload;
    }

    @Override
    public Class<? extends Event> type() {
        return ExternalTestEvent.class;
    }

    @Override
    public List<EventBus> eventBusTypes() {
        return List.of(EventBus.EXTERNAL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExternalTestEvent that = (ExternalTestEvent) o;
        return Objects.equals(id, that.id) && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payload);
    }

    @Override
    public String toString() {
        return "ExternalTestEvent{" +
                "id='" + id + '\'' +
                ", payload='" + payload + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
