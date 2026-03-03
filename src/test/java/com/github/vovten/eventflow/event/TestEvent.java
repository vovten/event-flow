package com.github.vovten.eventflow.event;

import java.util.List;

public record TestEvent(String id) implements Event {

    @Override
    public Class<? extends Event> type() {
        return TestEvent.class;
    }

    @Override
    public List<EventBus> eventBusTypes() {
        return List.of(EventBus.EXTERNAL);
    }
}
