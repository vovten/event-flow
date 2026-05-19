package io.github.vovten.eventflow.publisher;

/**
 * Error in event publisher operation
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class EventPublisherConfigException extends EventPublisherException {
    public EventPublisherConfigException(String message) {
        super(message);
    }

    public EventPublisherConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
