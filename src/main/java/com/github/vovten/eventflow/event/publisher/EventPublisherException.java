package com.github.vovten.eventflow.event.publisher;

/**
 * Error in event publisher operation
 *
 * @author Vladimir Aleshkov, 20.12.2024.
 */
public class EventPublisherException extends RuntimeException {
    public EventPublisherException(String message, Throwable cause) {
        super(message, cause);
    }
}
