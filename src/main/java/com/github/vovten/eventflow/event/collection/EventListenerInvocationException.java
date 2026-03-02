package com.github.vovten.eventflow.event.collection;

import com.github.vovten.eventflow.event.Event;

/**
 * Error when the dispatcher invokes a method to handle an event
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class EventListenerInvocationException extends RuntimeException {

    public EventListenerInvocationException(Object bean, Event event, Throwable cause) {
        super(String.format("Error when dispatcher invokes event listener. " +
                "Listener: %s, event: %s", bean.getClass(), event), cause);
    }
}
