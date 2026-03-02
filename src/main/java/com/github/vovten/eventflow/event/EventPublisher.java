package com.github.vovten.eventflow.event;

/**
 * Event publisher interface
 *
 * @author Vladimir Aleshkov, 20.11.2024.
 */
public interface EventPublisher {

    /**
     * Publish the event to the specified event bus
     *
     * @param event the event to publish
     */
    void publish(Event event);

    /**
     * @return  event bus
     */
    EventBus eventBus();
}
