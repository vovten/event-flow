package com.github.vovten.eventflow.transport;

/**
 * Error in event transport operation.
 * <p>
 * Thrown when a transport fails to send an event due to network issues,
 * broker unavailability, serialization errors, or timeouts.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class EventTransportException extends RuntimeException {
    
    /**
     * Create transport exception with message.
     *
     * @param message the error message
     */
    public EventTransportException(String message) {
        super(message);
    }
    
    /**
     * Create transport exception with message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public EventTransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
