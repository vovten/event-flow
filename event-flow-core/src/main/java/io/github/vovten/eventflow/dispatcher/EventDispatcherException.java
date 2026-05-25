package io.github.vovten.eventflow.dispatcher;

/**
 * Error in event dispatcher operation
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class EventDispatcherException extends RuntimeException {
    public EventDispatcherException(String message, Throwable cause) {
        super(message, cause);
    }
}
