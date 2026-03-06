package com.github.vovten.eventflow.dispatcher;

/**
 * Error in event dispatcher operation
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-20
 */
public class EventDispatcherException extends RuntimeException {
    public EventDispatcherException(String message, Throwable cause) {
        super(message, cause);
    }
}
