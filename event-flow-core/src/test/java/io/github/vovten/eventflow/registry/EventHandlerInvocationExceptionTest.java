package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventHandlerInvocationException}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@DisplayName("EventListenerInvocationException Tests")
class EventHandlerInvocationExceptionTest {

    @Test
    @DisplayName("Should create exception with listener and event")
    void shouldCreateExceptionWithListenerAndEvent() {
        // Arrange
        Object listener = new Object();
        TestEvent event = new TestEvent("test");

        // Act
        EventHandlerInvocationException exception = new EventHandlerInvocationException(listener, event, new RuntimeException("Cause"));

        // Assert
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains(listener.getClass().getName()));
        assertTrue(exception.getMessage().contains(event.type().getSimpleName()));
    }

    /**
     * Test event class.
     */
    private static final class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
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
