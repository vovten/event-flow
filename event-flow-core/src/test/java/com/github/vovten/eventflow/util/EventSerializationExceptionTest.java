package com.github.vovten.eventflow.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventSerializationException.
 */
@DisplayName("EventSerializationException Tests")
class EventSerializationExceptionTest {

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        String message = "Serialization failed";
        Throwable cause = new RuntimeException("JSON parsing error");
        EventSerializationException exception = new EventSerializationException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals("JSON parsing error", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("Should be RuntimeException")
    void shouldBeRuntimeException() {
        EventSerializationException exception = new EventSerializationException("Error", new RuntimeException());
        assertTrue(exception instanceof RuntimeException);
    }
}
