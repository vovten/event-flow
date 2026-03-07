package com.github.vovten.eventflow.publisher;

/**
 * Error in event publisher operation
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-20
 */
public class EventPublisherConfigException extends EventPublisherException {
    public EventPublisherConfigException(String message) {
        super(message);
    }

    public EventPublisherConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
