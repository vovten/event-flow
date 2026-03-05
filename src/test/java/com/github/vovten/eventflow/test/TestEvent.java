package com.github.vovten.eventflow.test;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Base test event for unit and integration tests
 */
public class TestEvent implements Event {
    
    private String id;
    private String message;
    private LocalDateTime timestamp;
    
    public TestEvent() {
        this(UUID.randomUUID().toString(), "Test event message", LocalDateTime.now());
    }
    
    public TestEvent(String id, String message, LocalDateTime timestamp) {
        this.id = id;
        this.message = message;
        this.timestamp = timestamp;
    }
    
    public static TestEvent create() {
        return new TestEvent();
    }
    
    public static TestEvent create(String message) {
        return new TestEvent(UUID.randomUUID().toString(), message, LocalDateTime.now());
    }
    
    public static TestEvent create(String id, String message) {
        return new TestEvent(id, message, LocalDateTime.now());
    }

    public static TestEvent create(String id, String message, LocalDateTime dateTime) {
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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
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
                ", timestamp=" + timestamp +
                '}';
    }
}
