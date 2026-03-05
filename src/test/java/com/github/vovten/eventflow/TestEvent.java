package com.github.vovten.eventflow;

import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;

import java.util.List;

public record TestEvent(String id) implements Event {

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
}
