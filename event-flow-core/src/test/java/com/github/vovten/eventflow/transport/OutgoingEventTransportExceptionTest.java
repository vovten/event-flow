package com.github.vovten.eventflow.transport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link OutgoingEventTransportException}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("OutgoingEventTransportException Tests")
class OutgoingEventTransportExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void shouldCreateExceptionWithMessage() {
        // Act
        OutgoingEventTransportException exception = new OutgoingEventTransportException("Test message");

        // Assert
        assertNotNull(exception);
        assertEquals("Test message", exception.getMessage());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        // Arrange
        Throwable cause = new RuntimeException("Cause");

        // Act
        OutgoingEventTransportException exception = new OutgoingEventTransportException("Test message", cause);

        // Assert
        assertNotNull(exception);
        assertEquals("Test message", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
