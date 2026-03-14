package com.github.vovten.eventflow.test;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Test event for both internal and external buses
 */
public class CompositeTestEvent extends AbstractTraceableEvent {

    private String id;
    private String content;

    public CompositeTestEvent() {
        this(UUID.randomUUID().toString(), "Composite test event content");
    }

    public CompositeTestEvent(String id, String content) {
        super();
        this.id = id;
        this.content = content;
    }

    public CompositeTestEvent(UUID uid, UUID traceId, String id, String content, LocalDateTime timestamp) {
        super(uid, traceId, timestamp);
        this.id = id;
        this.content = content;
    }

    public CompositeTestEvent(String id, String content, LocalDateTime timestamp) {
        super(UUID.randomUUID(), UUID.randomUUID(), timestamp);
        this.id = id;
        this.content = content;
    }

    public static CompositeTestEvent create() {
        return new CompositeTestEvent();
    }

    public static CompositeTestEvent create(String content) {
        return new CompositeTestEvent(UUID.randomUUID().toString(), content);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    /**
     * Alias for content to maintain consistency with TestEvent
     */
    public String getMessage() {
        return content;
    }

    @Override
    public Class<? extends Event> type() {
        return CompositeTestEvent.class;
    }

    @Override
    public List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class, ExternalEventChannel.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeTestEvent that = (CompositeTestEvent) o;
        return Objects.equals(id, that.id) && Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, content);
    }

    @Override
    public String toString() {
        return "CompositeTestEvent{" +
                "id='" + id + '\'' +
                ", content='" + content + '\'' +
                ", uid=" + uid() +
                ", timestamp=" + this.occurredAt() +
                '}';
    }
}
