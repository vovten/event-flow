package com.github.vovten.eventflow.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventSerializationException.
 */
class EventSerializationExceptionTest {

    @Test
    void testConstructorWithMessageAndCause() {
        String message = "Serialization failed";
        Throwable cause = new RuntimeException("JSON parsing error");
        EventSerializationException exception = new EventSerializationException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals("JSON parsing error", exception.getCause().getMessage());
    }

    @Test
    void testIsRuntimeException() {
        EventSerializationException exception = new EventSerializationException("Error", new RuntimeException());
        assertTrue(exception instanceof RuntimeException);
    }
}
