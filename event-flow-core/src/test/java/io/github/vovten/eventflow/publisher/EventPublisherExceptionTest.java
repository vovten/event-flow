package io.github.vovten.eventflow.publisher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventPublisherException}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("EventPublisherException Tests")
class EventPublisherExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void shouldCreateExceptionWithMessage() {
        // Act
        EventPublisherException exception = new EventPublisherException("Test message");

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
        EventPublisherException exception = new EventPublisherException("Test message", cause);

        // Assert
        assertNotNull(exception);
        assertEquals("Test message", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
