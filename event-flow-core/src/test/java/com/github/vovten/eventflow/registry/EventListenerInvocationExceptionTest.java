package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventListenerInvocationException}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("EventListenerInvocationException Tests")
class EventListenerInvocationExceptionTest {

    @Test
    @DisplayName("Should create exception with listener and event")
    void shouldCreateExceptionWithListenerAndEvent() {
        // Arrange
        Object listener = new Object();
        TestEvent event = new TestEvent("test");

        // Act
        EventListenerInvocationException exception = new EventListenerInvocationException(listener, event, new RuntimeException("Cause"));

        // Assert
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains(listener.getClass().getName()));
        assertTrue(exception.getMessage().contains(event.type().getSimpleName()));
    }

    /**
     * Test event class.
     */
    private static class TestEvent implements Event {
        private final String data;

        public TestEvent(String data) {
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\"}";
        }
    }
}
