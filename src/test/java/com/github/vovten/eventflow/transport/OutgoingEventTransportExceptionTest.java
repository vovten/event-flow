package com.github.vovten.eventflow.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OutgoingEventTransportException.
 */
class OutgoingEventTransportExceptionTest {

    @Test
    void testConstructorWithMessage() {
        String message = "Transport failed";
        OutgoingEventTransportException exception = new OutgoingEventTransportException(message);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructorWithMessageAndCause() {
        String message = "Transport failed";
        Throwable cause = new RuntimeException("Underlying cause");
        OutgoingEventTransportException exception = new OutgoingEventTransportException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals("Underlying cause", exception.getCause().getMessage());
    }

    @Test
    void testIsRuntimeException() {
        OutgoingEventTransportException exception = new OutgoingEventTransportException("Error");
        assertTrue(exception instanceof RuntimeException);
    }
}
