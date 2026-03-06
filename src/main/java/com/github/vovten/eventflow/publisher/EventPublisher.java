package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;

/**
 * Event publisher interface.
 *
 * @author Vladimir Aleshkov
 * @since 2024-11-20
 */
public interface EventPublisher {

    /**
     * Publish the event.
     *
     * @param event the event to publish
     */
    void publish(Event event);
}
