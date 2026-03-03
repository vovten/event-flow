package com.github.vovten.eventflow.publisher;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventPublisher;
import com.github.vovten.eventflow.EventBus;

import java.util.concurrent.BlockingDeque;

/**
 * Publishes events to the internal bus.
 * Used for event exchange only within a single application.
 *
 * @author Vladimir Aleshkov, 20.11.2024.
 */
@Component
@ConditionalOnProperty(name = "event.internal.enabled", havingValue = "true")
public class InternalEventPublisher implements EventPublisher {

    private final BlockingDeque<Event> eventQueue;

    public InternalEventPublisher(BlockingDeque<Event> eventQueue) {
        this.eventQueue = eventQueue;
    }

    @Override
    public void publish(Event event) {
        if (!event.eventBusTypes().contains(EventBus.INTERNAL)) {
            throw new IllegalArgumentException("This publisher only supports INTERNAL event bus");
        }
        try {
            eventQueue.add(event);
        } catch (Exception e) {
            throw new EventPublisherException("Error adding event to queue: " + event, e);
        }
    }

    @Override
    public EventBus eventBus() {
        return EventBus.INTERNAL;
    }
}
