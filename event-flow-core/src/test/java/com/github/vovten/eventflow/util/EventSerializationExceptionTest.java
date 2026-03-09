package com.github.vovten.eventflow.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventSerializationException}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("EventSerializationException Tests")
class EventSerializationExceptionTest {

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        // Arrange
        Throwable cause = new RuntimeException("Cause");

        // Act
        EventSerializationException exception = new EventSerializationException("Test message", cause);

        // Assert
        assertNotNull(exception);
        assertEquals("Test message", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
