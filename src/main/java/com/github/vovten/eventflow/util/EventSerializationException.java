// File: EventSerializationException.java
package com.github.vovten.eventflow.util;

/**
 * Event serialization error
 *
 * @author Vladimir Aleshkov
 * @since 2024-11-21
 */
public class EventSerializationException extends RuntimeException {
    public EventSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
