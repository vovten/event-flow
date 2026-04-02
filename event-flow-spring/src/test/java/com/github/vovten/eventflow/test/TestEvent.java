package com.github.vovten.eventflow.test;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.event.Event;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Base test event for unit and integration tests
 */
public class TestEvent extends AbstractTraceableEvent {

    private String id;
    private String message;

    public TestEvent() {
        this(UUID.randomUUID().toString(), "Test event message");
    }

    public TestEvent(String id, String message) {
        super();
        this.id = id;
        this.message = message;
    }

    public TestEvent(UUID uid, String id, String message, Instant timestamp) {
        super(uid, UUID.randomUUID(), timestamp);
        this.id = id;
        this.message = message;
    }

    public TestEvent(UUID uid, UUID traceId, String id, String message, Instant timestamp) {
        super(uid, traceId, timestamp);
        this.id = id;
        this.message = message;
    }

    public TestEvent(String id, String message, Instant timestamp) {
        super(UUID.randomUUID(), UUID.randomUUID(), timestamp);
        this.id = id;
        this.message = message;
    }

    public static TestEvent create() {
        return new TestEvent();
    }

    public static TestEvent create(String message) {
        return new TestEvent(UUID.randomUUID().toString(), message);
    }

    public static TestEvent create(String id, String message) {
        return new TestEvent(id, message);
    }

    public static TestEvent create(String id, String message, Instant dateTime) {
        return new TestEvent(id, message, dateTime);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public Class<? extends Event> type() {
        return TestEvent.class;
    }

    @Override
    public List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestEvent testEvent = (TestEvent) o;
        return Objects.equals(id, testEvent.id) &&
               Objects.equals(message, testEvent.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, message);
    }

    @Override
    public String toString() {
        return "TestEvent{" +
                "id='" + id + '\'' +
                ", message='" + message + '\'' +
                ", uid=" + uid() +
                ", timestamp=" + this.occurredAt() +
                '}';
    }
}
