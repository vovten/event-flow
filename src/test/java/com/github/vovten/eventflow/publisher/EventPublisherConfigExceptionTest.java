package com.github.vovten.eventflow.publisher;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventPublisherConfigException.
 */
class EventPublisherConfigExceptionTest {

    @Test
    void testConstructorWithMessage() {
        String message = "Configuration error";
        EventPublisherConfigException exception = new EventPublisherConfigException(message);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructorWithMessageAndCause() {
        String message = "Configuration error";
        Throwable cause = new IllegalArgumentException("Invalid config");
        EventPublisherConfigException exception = new EventPublisherConfigException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals("Invalid config", exception.getCause().getMessage());
    }

    @Test
    void testIsEventPublisherException() {
        EventPublisherConfigException exception = new EventPublisherConfigException("Error");
        assertTrue(exception instanceof EventPublisherException);
    }

    @Test
    void testIsRuntimeException() {
        EventPublisherConfigException exception = new EventPublisherConfigException("Error");
        assertTrue(exception instanceof RuntimeException);
    }
}
