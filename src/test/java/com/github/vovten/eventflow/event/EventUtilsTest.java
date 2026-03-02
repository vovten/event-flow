package com.github.vovten.eventflow.event;

import com.github.vovten.eventflow.event.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventUtils
 */
class EventUtilsTest {

    @Test
    @DisplayName("Should convert event to JSON string")
    void shouldConvertEventToJsonString() {
        // given
        TestEvent event = TestEvent.create("Test message");

        // when
        String json = EventUtils.toJson(event);

        // then
        assertNotNull(json);
        assertTrue(json.contains("Test message"));
        assertTrue(json.contains("TestEvent"));
    }

    @Test
    @DisplayName("Should serialize all event fields to JSON")
    void shouldSerializeAllEventFieldsToJson() {
        // given
        TestEvent event = TestEvent.create("test-id-123", "Test payload");

        // when
        String json = EventUtils.toJson(event);

        // then
        assertNotNull(json);
        assertTrue(json.contains("test-id-123"));
        assertTrue(json.contains("Test payload"));
    }

    @Test
    @DisplayName("Should handle complex event objects")
    void shouldHandleComplexEventObjects() {
        // given
        ComplexEvent event = new ComplexEvent(
            "complex-id",
            "Complex event",
            new NestedData("nested-value", 42)
        );

        // when
        String json = EventUtils.toJson(event);

        // then
        assertNotNull(json);
        assertTrue(json.contains("complex-id"));
        assertTrue(json.contains("nested-value"));
        assertTrue(json.contains("42"));
    }

    @Test
    @DisplayName("Should throw EventSerializationException for non-serializable objects")
    void shouldThrowEventSerializationExceptionForNonSerializableObjects() {
        // given
        Event nonSerializableEvent = new NonSerializableEvent();

        // when & then
        EventSerializationException exception = assertThrows(
            EventSerializationException.class,
            () -> EventUtils.toJson(nonSerializableEvent)
        );
        assertTrue(exception.getMessage().contains("Error converting object to json"));
    }

    // Test helper class with nested object
    static class ComplexEvent implements Event {
        private String id;
        private String message;
        private NestedData data;

        ComplexEvent(String id, String message, NestedData data) {
            this.id = id;
            this.message = message;
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return ComplexEvent.class;
        }
    }

    static class NestedData {
        private String value;
        private int number;

        NestedData(String value, int number) {
            this.value = value;
            this.number = number;
        }
    }

    // Test helper class that cannot be serialized
    static class NonSerializableEvent implements Event {
        private transient Object nonSerializableField = new Object();

        @Override
        public Class<? extends Event> type() {
            return NonSerializableEvent.class;
        }
    }
}
