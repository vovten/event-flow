package com.github.vovten.eventflow.serialization;

/**
 * Exception thrown when event serialization or deserialization fails.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
public class EventSerializationException extends RuntimeException {

    public EventSerializationException(String message) {
        super(message);
    }

    public EventSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
