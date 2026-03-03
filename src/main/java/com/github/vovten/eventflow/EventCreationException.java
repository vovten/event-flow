package com.github.vovten.eventflow;

/**
 * Exception thrown when errors occur during event creation
 *
 * @author Vladimir Aleshkov, 27.11.2024.
 */
public class EventCreationException extends RuntimeException {
    public EventCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
