package com.github.vovten.eventflow.publisher;

/**
 * Error in event publisher operation
 *
 * @author Vladimir Aleshkov, 20.12.2024.
 */
public class EventPublisherConfigException extends EventPublisherException {
    public EventPublisherConfigException(String message) {
        super(message);
    }

    public EventPublisherConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
