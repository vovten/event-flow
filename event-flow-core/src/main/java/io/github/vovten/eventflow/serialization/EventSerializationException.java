package io.github.vovten.eventflow.serialization;

/**
 * Exception thrown when event serialization or deserialization fails.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class EventSerializationException extends RuntimeException {

    public EventSerializationException(String message) {
        super(message);
    }

    public EventSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
