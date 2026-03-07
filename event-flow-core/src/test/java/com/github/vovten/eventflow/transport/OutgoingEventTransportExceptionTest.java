package com.github.vovten.eventflow.transport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OutgoingEventTransportException.
 */
@DisplayName("OutgoingEventTransportException Tests")
class OutgoingEventTransportExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void shouldCreateExceptionWithMessage() {
        String message = "Transport failed";
        OutgoingEventTransportException exception = new OutgoingEventTransportException(message);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        String message = "Transport failed";
        Throwable cause = new RuntimeException("Underlying cause");
        OutgoingEventTransportException exception = new OutgoingEventTransportException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals("Underlying cause", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("Should be RuntimeException")
    void shouldBeRuntimeException() {
        OutgoingEventTransportException exception = new OutgoingEventTransportException("Error");
        assertTrue(exception instanceof RuntimeException);
    }
}
