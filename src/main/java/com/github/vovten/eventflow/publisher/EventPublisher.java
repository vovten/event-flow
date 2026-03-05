package com.github.vovten.eventflow;

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
     * The type of bus used when publishing events
     *
     * @return  event bus
     */
    EventBus eventBus();
}
