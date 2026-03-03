package com.github.vovten.eventflow.test;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventBus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Test event for both internal and external buses
 */
public class CompositeTestEvent implements Event {

    private String id;
    private String content;
    private LocalDateTime timestamp;

    public CompositeTestEvent() {
        this(UUID.randomUUID().toString(), "Composite test event content", LocalDateTime.now());
    }

    public CompositeTestEvent(String id, String content, LocalDateTime timestamp) {
        this.id = id;
        this.content = content;
        this.timestamp = timestamp;
    }

    public static CompositeTestEvent create() {
        return new CompositeTestEvent();
    }

    public static CompositeTestEvent create(String content) {
        return new CompositeTestEvent(UUID.randomUUID().toString(), content, LocalDateTime.now());
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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
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
    public List<EventBus> eventBusTypes() {
        return List.of(EventBus.INTERNAL, EventBus.EXTERNAL);
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
                ", timestamp=" + timestamp +
                '}';
    }
}
