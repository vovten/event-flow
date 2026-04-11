package io.github.vovten.eventflow.transport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link TransportException}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("TransportException Tests")
class TransportExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void shouldCreateExceptionWithMessage() {
        // Act
        TransportException exception = new TransportException("Test message");

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
        TransportException exception = new TransportException("Test message", cause);

        // Assert
        assertNotNull(exception);
        assertEquals("Test message", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
