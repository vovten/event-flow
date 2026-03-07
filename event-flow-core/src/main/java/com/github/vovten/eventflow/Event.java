package com.github.vovten.eventflow;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.util.EventUtils;

import java.util.List;

/**
 * An event that occurs in the application and can be delivered to all interested parties
 * (components within the application, components of third-party applications (microservices)).
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-02
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Event {

    /**
     * @return the event type
     */
    Class<? extends Event> type();

    /**
     * List of channel classes this event should be published to.
     * Uses class types for compile-time safety.
     * By default, the event is published to the internal channel.
     *
     * @return list of channel classes
     */
    default List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class);
    }

    /**
     * @return the event as JSON
     */
    default String asJson() {
        return EventUtils.toJson(this);
    }
}
