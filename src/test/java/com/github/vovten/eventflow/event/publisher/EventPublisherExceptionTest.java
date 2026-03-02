package com.github.vovten.eventflow.event.publisher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventPublisherException
 */
class EventPublisherExceptionTest {

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        // given
        Throwable cause = new RuntimeException("Original cause");

        // when
        EventPublisherException exception = new EventPublisherException("Test message", cause);

        // then
        assertEquals("Test message", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should preserve cause stack trace")
    void shouldPreserveCauseStackTrace() {
        // given
        IllegalStateException cause = new IllegalStateException("Illegal state");

        // when
        EventPublisherException exception = new EventPublisherException("Publisher error", cause);

        // then
        assertNotNull(exception.getCause());
        assertEquals("Illegal state", exception.getCause().getMessage());
        assertTrue(exception.getCause() instanceof IllegalStateException);
    }
}
