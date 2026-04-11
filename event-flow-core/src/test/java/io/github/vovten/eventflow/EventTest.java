package io.github.vovten.eventflow;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link Event} interface default methods.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("Event Tests")
class EventTest {

    @Test
    @DisplayName("Should return event type")
    void shouldReturnEventType() {
        // Arrange
        TestEvent event = new TestEvent("test");

        // Assert
        assertEquals(TestEvent.class, event.type());
    }

    @Test
    @DisplayName("Should return JSON representation")
    void shouldReturnJsonRepresentation() {
        // Arrange
        TestEvent event = new TestEvent("test-data");

        // Act
        String json = event.asJson();

        // Assert
        assertNotNull(json);
        assertTrue(json.contains("test-data"));
    }

    /**
     * Test event implementation.
     */
    private static class TestEvent extends AbstractTraceableEvent {
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
