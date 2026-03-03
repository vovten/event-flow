package com.github.vovten.eventflow.test;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventBus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Test event for replicas dispatcher
 */
public class ReplicasTestEvent implements Event {

    private String id;
    private String data;
    private LocalDateTime timestamp;

    public ReplicasTestEvent() {
        this(UUID.randomUUID().toString(), "Replicas test event data", LocalDateTime.now());
    }

    public ReplicasTestEvent(String id, String data, LocalDateTime timestamp) {
        this.id = id;
        this.data = data;
        this.timestamp = timestamp;
    }

    public static ReplicasTestEvent create() {
        return new ReplicasTestEvent();
    }

    public static ReplicasTestEvent create(String data) {
        return new ReplicasTestEvent(UUID.randomUUID().toString(), data, LocalDateTime.now());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public Class<? extends Event> type() {
        return ReplicasTestEvent.class;
    }

    @Override
    public List<EventBus> eventBusTypes() {
        return List.of(EventBus.EXTERNAL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicasTestEvent that = (ReplicasTestEvent) o;
        return Objects.equals(id, that.id) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, data);
    }

    @Override
    public String toString() {
        return "ReplicasTestEvent{" +
                "id='" + id + '\'' +
                ", data='" + data + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
