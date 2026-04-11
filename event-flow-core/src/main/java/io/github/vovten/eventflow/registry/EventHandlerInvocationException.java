package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.event.Event;

/**
 * Error when the dispatcher invokes a method to handle an event
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-07
 */
public class EventHandlerInvocationException extends RuntimeException {

    public EventHandlerInvocationException(Object bean, Event event, Throwable cause) {
        super(String.format("Error when dispatcher invokes event listener. " +
                "Listener: %s, event: %s", bean.getClass(), event), cause);
    }
}
