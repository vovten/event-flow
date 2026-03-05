package com.github.vovten.eventflow.transport;

/**
 * Error in event transport operation
 *
 * @author Vladimir Aleshkov
 */
public class EventTransportException extends RuntimeException {
    public EventTransportException(String message) {
        super(message);
    }
}
