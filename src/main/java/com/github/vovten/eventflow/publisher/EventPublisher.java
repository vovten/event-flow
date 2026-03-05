package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;

/**
 * Event publisher interface.
 *
 * @author Vladimir Aleshkov
 * @since 20.11.2024
 */
public interface EventPublisher {

    /**
     * Publish the event.
     *
     * @param event the event to publish
     */
    void publish(Event event);
}
