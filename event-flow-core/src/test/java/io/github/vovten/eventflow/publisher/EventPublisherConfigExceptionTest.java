package io.github.vovten.eventflow.publisher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventPublisherConfigException}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("EventPublisherConfigException Tests")
class EventPublisherConfigExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void shouldCreateExceptionWithMessage() {
        // Act
        EventPublisherConfigException exception = new EventPublisherConfigException("Test message");

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
        EventPublisherConfigException exception = new EventPublisherConfigException("Test message", cause);

        // Assert
        assertNotNull(exception);
        assertEquals("Test message", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
