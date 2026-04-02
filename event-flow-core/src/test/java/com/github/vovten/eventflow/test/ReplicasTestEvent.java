package com.github.vovten.eventflow.test;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Test event for replicas dispatcher
 */
public class ReplicasTestEvent extends AbstractTraceableEvent {

    private String id;
    private String data;

    public ReplicasTestEvent() {
        this(UUID.randomUUID().toString(), "Replicas test event data");
    }

    public ReplicasTestEvent(String id, String data) {
        super();
        this.id = id;
        this.data = data;
    }

    public ReplicasTestEvent(UUID uid, UUID traceId, String id, String data, Instant timestamp) {
        super(uid, traceId, timestamp);
        this.id = id;
        this.data = data;
    }

    public ReplicasTestEvent(String id, String data, Instant timestamp) {
        super(UUID.randomUUID(), UUID.randomUUID(), timestamp);
        this.id = id;
        this.data = data;
    }

    public static ReplicasTestEvent create() {
        return new ReplicasTestEvent();
    }

    public static ReplicasTestEvent create(String data) {
        return new ReplicasTestEvent(UUID.randomUUID().toString(), data);
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

    @Override
    public Class<? extends Event> type() {
        return ReplicasTestEvent.class;
    }

    @Override
    public List<Class<? extends EventChannel>> channels() {
        return List.of(ExternalEventChannel.class);
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
                ", uid=" + uid() +
                ", timestamp=" + this.occurredAt() +
                '}';
    }
}
