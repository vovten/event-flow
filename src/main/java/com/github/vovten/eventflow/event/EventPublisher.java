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
     * @param eventBus the target event bus
     */
    void publish(Event event, EventBus eventBus);

    /**
     * @return  event bus
     */
    EventBus eventBus();
}
