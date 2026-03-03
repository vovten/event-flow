package com.github.vovten.eventflow;

import com.github.vovten.eventflow.util.EventSerializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventSerializationException
 */
class EventSerializationExceptionTest {

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        // given
        Throwable cause = new RuntimeException("JSON parsing error");

        // when
        EventSerializationException exception = new EventSerializationException("Serialization failed", cause);

        // then
        assertEquals("Serialization failed", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should preserve cause stack trace")
    void shouldPreserveCauseStackTrace() {
        // given
        com.fasterxml.jackson.core.JsonProcessingException cause = 
            new com.fasterxml.jackson.core.JsonProcessingException("Invalid JSON") {};

        // when
        EventSerializationException exception = new EventSerializationException("Error", cause);

        // then
        assertNotNull(exception.getCause());
        assertTrue(exception.getCause() instanceof com.fasterxml.jackson.core.JsonProcessingException);
    }
}
