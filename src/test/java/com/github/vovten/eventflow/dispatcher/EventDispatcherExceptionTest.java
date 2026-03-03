package com.github.vovten.eventflow.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventDispatcherException
 */
class EventDispatcherExceptionTest {

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        // given
        Throwable cause = new RuntimeException("Original cause");

        // when
        EventDispatcherException exception = new EventDispatcherException("Test message", cause);

        // then
        assertEquals("Test message", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should preserve cause stack trace")
    void shouldPreserveCauseStackTrace() {
        // given
        IllegalArgumentException cause = new IllegalArgumentException("Invalid argument");

        // when
        EventDispatcherException exception = new EventDispatcherException("Dispatcher error", cause);

        // then
        assertNotNull(exception.getCause());
        assertEquals("Invalid argument", exception.getCause().getMessage());
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
    }
}
