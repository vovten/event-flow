package com.github.vovten.eventflow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventCreationException
 */
class EventCreationExceptionTest {

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        // given
        Throwable cause = new RuntimeException("Builder error");

        // when
        EventCreationException exception = new EventCreationException("Event creation failed", cause);

        // then
        assertEquals("Event creation failed", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should preserve cause stack trace")
    void shouldPreserveCauseStackTrace() {
        // given
        ReflectiveOperationException cause = new ReflectiveOperationException("Reflection failed");

        // when
        EventCreationException exception = new EventCreationException("Cannot create event", cause);

        // then
        assertNotNull(exception.getCause());
        assertEquals("Reflection failed", exception.getCause().getMessage());
    }
}
