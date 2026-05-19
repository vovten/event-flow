package io.github.vovten.eventflow.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventDispatcherException.
 * @since 1.0.0
 */
@DisplayName("EventDispatcherException Tests")
class EventDispatcherExceptionTest {

    @Test
    @DisplayName("Should create exception with message and cause")
    void shouldCreateExceptionWithMessageAndCause() {
        String message = "Dispatcher error";
        Throwable cause = new RuntimeException("Underlying cause");
        EventDispatcherException exception = new EventDispatcherException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals("Underlying cause", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("Should be RuntimeException")
    void shouldBeRuntimeException() {
        EventDispatcherException exception = new EventDispatcherException("Error", new RuntimeException());
        assertTrue(exception instanceof RuntimeException);
    }
}
