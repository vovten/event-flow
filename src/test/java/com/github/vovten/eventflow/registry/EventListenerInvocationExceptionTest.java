package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventListenerInvocationException
 */
class EventListenerInvocationExceptionTest {

    @Test
    @DisplayName("Should create exception with listener and event details")
    void shouldCreateExceptionWithListenerAndEventDetails() {
        // given
        Object listener = new Object();
        Event event = TestEvent.create("Test event");
        Throwable cause = new RuntimeException("Invocation failed");

        // when
        EventListenerInvocationException exception = 
            new EventListenerInvocationException(listener, event, cause);

        // then
        assertNotNull(exception.getMessage());
        assertTrue(exception.getMessage().contains(listener.getClass().toString()));
        assertTrue(exception.getMessage().contains("Test event"));
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should preserve cause stack trace")
    void shouldPreserveCauseStackTrace() {
        // given
        Object listener = new Object();
        Event event = TestEvent.create();
        IllegalAccessException cause = new IllegalAccessException("Access denied");

        // when
        EventListenerInvocationException exception = 
            new EventListenerInvocationException(listener, event, cause);

        // then
        assertNotNull(exception.getCause());
        assertEquals("Access denied", exception.getCause().getMessage());
        assertTrue(exception.getCause() instanceof IllegalAccessException);
    }
}
