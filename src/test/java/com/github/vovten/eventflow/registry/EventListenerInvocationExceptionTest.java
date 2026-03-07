package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventListenerInvocationException.
 */
class EventListenerInvocationExceptionTest {

    @Test
    void testConstructorWithListenerEventAndCause() {
        Object listener = new TestListener();
        Event event = new TestEvent();
        Throwable cause = new RuntimeException("Invocation failed");
        EventListenerInvocationException exception = new EventListenerInvocationException(listener, event, cause);

        assertTrue(exception.getMessage().contains("TestListener"));
        assertTrue(exception.getMessage().contains("TestEvent"));
        assertEquals(cause, exception.getCause());
        assertEquals("Invocation failed", exception.getCause().getMessage());
    }

    @Test
    void testIsRuntimeException() {
        Object listener = new TestListener();
        Event event = new TestEvent();
        EventListenerInvocationException exception = new EventListenerInvocationException(listener, event, new RuntimeException());

        assertTrue(exception instanceof RuntimeException);
    }

    static class TestListener {
    }
}
