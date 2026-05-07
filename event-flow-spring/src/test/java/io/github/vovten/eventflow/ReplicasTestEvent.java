package io.github.vovten.eventflow;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.ExternalEventChannel;

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
        super();
        this.id = UUID.randomUUID().toString();
        this.data = "Replicas test event data";
    }

    public ReplicasTestEvent(String id, String data) {
        super();
        this.id = id;
        this.data = data;
    }

    public ReplicasTestEvent(String data) {
        super();
        this.id = UUID.randomUUID().toString();
        this.data = data;
    }

    public ReplicasTestEvent(UUID uid, UUID processId, String id, String data, Instant timestamp) {
        super(uid, processId, timestamp);
        this.id = id;
        this.data = data;
    }

    public ReplicasTestEvent(String id, String data, Instant timestamp) {
        super(timestamp);
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
                ", eventId=" + eventId() +
                ", timestamp=" + this.occurredAt() +
                '}';
    }
}