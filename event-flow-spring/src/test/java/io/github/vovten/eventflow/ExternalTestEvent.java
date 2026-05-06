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
 * Test event for external bus (Kafka)
 */
public class ExternalTestEvent extends AbstractTraceableEvent {

    private String id;
    private String payload;

    public ExternalTestEvent() {
        super();
        this.id = UUID.randomUUID().toString();
        this.payload = "External test event payload";
    }

    public ExternalTestEvent(String id, String payload) {
        super();
        this.id = id;
        this.payload = payload;
    }
    public ExternalTestEvent(String payload) {
        super();
        this.id = UUID.randomUUID().toString();
        this.payload = payload;
    }

    public ExternalTestEvent(UUID uid, UUID processId, String id, String payload, Instant timestamp) {
        super(uid, processId, timestamp);
        this.id = id;
        this.payload = payload;
    }

    public ExternalTestEvent(String id, String payload, Instant timestamp) {
        super(timestamp);
        this.id = id;
        this.payload = payload;
    }

    public static ExternalTestEvent create() {
        return new ExternalTestEvent();
    }

    public static ExternalTestEvent create(String payload) {
        return new ExternalTestEvent(UUID.randomUUID().toString(), payload);
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
    public List<Class<? extends EventChannel>> channels() {
        return List.of(ExternalEventChannel.class);
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
                ", eventId=" + eventId() +
                ", timestamp=" + this.occurredAt() +
                '}';
    }
}