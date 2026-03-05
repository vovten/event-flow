package com.github.vovten.eventflow;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
     * List of event buses where this event will be published.
     * By default, the event is published only to the internal bus {@link EventBus#INTERNAL}.
     */
    default List<EventBus> eventBusTypes() {
        return List.of(EventBus.INTERNAL);
    }

    /**
     * @return the event as JSON
     */
    default String asJson() {
        return EventUtils.toJson(this);
    }
}
