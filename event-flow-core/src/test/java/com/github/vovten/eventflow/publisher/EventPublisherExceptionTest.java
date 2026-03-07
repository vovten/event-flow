package com.github.vovten.eventflow.publisher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventPublisherException.
 */
@DisplayName("EventPublisherException Tests")
class EventPublisherExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void shouldCreateExceptionWithMessage() {
        String message = "Publisher error";
        EventPublisherException exception = new EventPublisherException(message);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        String message = "Publisher error";
        Throwable cause = new RuntimeException("Underlying cause");
        EventPublisherException exception = new EventPublisherException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals("Underlying cause", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("Should be RuntimeException")
    void shouldBeRuntimeException() {
        EventPublisherException exception = new EventPublisherException("Error");
        assertTrue(exception instanceof RuntimeException);
    }
}
