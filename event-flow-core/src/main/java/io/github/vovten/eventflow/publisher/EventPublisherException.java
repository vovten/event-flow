package io.github.vovten.eventflow.publisher;

/**
 * Error in event publisher operation
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class EventPublisherException extends RuntimeException {
    public EventPublisherException(String message) {
        super(message);
    }

    public EventPublisherException(String message, Throwable cause) {
        super(message, cause);
    }
}
