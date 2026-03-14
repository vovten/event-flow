package com.github.vovten.eventflow;

import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;

import java.util.List;
import java.util.Objects;

/**
 * Test event record-based for unit and integration tests
 */
public class TestEvent extends AbstractTraceableEvent {

    private final String id;

    public TestEvent(String id) {
        super();
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public Class<? extends Event> type() {
        return TestEvent.class;
    }

    @Override
    public List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class, ExternalEventChannel.class);
    }

    public String getMessage() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestEvent testEvent = (TestEvent) o;
        return Objects.equals(id, testEvent.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "TestEvent{id='" + id + "', uid=" + uid() + ", traceId=" + traceId() + ", timestamp=" + occurredAt() + "}";
    }
}
