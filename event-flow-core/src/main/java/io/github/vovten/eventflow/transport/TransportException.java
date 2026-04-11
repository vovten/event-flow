package io.github.vovten.eventflow.transport;

/**
 * Error in transport operation.
 * <p>
 * Thrown when a transport fails to send or receive an event due to network issues,
 * broker unavailability, serialization errors, or timeouts.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class TransportException extends RuntimeException {

    /**
     * Create transport exception with message.
     *
     * @param message the error message
     */
    public TransportException(String message) {
        super(message);
    }

    /**
     * Create transport exception with message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
