package com.github.vovten.eventflow.publisher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventPublisherConfigException.
 */
@DisplayName("EventPublisherConfigException Tests")
class EventPublisherConfigExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void shouldCreateExceptionWithMessage() {
        String message = "Configuration error";
        EventPublisherConfigException exception = new EventPublisherConfigException(message);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        String message = "Configuration error";
        Throwable cause = new IllegalArgumentException("Invalid config");
        EventPublisherConfigException exception = new EventPublisherConfigException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals("Invalid config", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("Should be EventPublisherException")
    void shouldBeEventPublisherException() {
        EventPublisherConfigException exception = new EventPublisherConfigException("Error");
        assertTrue(exception instanceof EventPublisherException);
    }

    @Test
    @DisplayName("Should be RuntimeException")
    void shouldBeRuntimeException() {
        EventPublisherConfigException exception = new EventPublisherConfigException("Error");
        assertTrue(exception instanceof RuntimeException);
    }
}
