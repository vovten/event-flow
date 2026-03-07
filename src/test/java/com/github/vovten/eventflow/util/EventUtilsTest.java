package com.github.vovten.eventflow.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.vovten.eventflow.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventUtils.
 */
@DisplayName("EventUtils Tests")
class EventUtilsTest {

    @Test
    @DisplayName("Should convert simple event to JSON")
    void shouldConvertSimpleEventToJson() {
        SimpleEvent event = new SimpleEvent("test-id", 42);

        String json = EventUtils.toJson(event);

        assertNotNull(json);
        assertTrue(json.contains("test-id"));
        assertTrue(json.contains("42"));
    }

    @Test
    @DisplayName("Should convert complex event to JSON")
    void shouldConvertComplexEventToJson() {
        ComplexEvent event = new ComplexEvent("order-123", List.of("item1", "item2"), LocalDateTime.of(2024, 1, 1, 10, 0));

        String json = EventUtils.toJson(event);

        assertNotNull(json);
        assertTrue(json.contains("order-123"));
        assertTrue(json.contains("item1"));
        assertTrue(json.contains("item2"));
    }

    @Test
    @DisplayName("Should convert JSON to event")
    void shouldConvertJsonToEvent() {
        String json = "{\"id\":\"test-id\",\"value\":42,\"@class\":\"com.github.vovten.eventflow.util.EventUtilsTest$SimpleEvent\"}";

        SimpleEvent event = EventUtils.fromJson(json, SimpleEvent.class);

        assertEquals("test-id", event.id);
        assertEquals(42, event.value);
    }

    @Test
    @DisplayName("Should throw exception for invalid JSON")
    void shouldThrowExceptionForInvalidJson() {
        String invalidJson = "{invalid json}";

        assertThrows(EventSerializationException.class, () ->
                EventUtils.fromJson(invalidJson, SimpleEvent.class));
    }

    @Test
    @DisplayName("Should throw exception for null JSON")
    void shouldThrowExceptionForNullJson() {
        assertThrows(IllegalArgumentException.class, () ->
                EventUtils.fromJson(null, SimpleEvent.class));
    }

    @Test
    @DisplayName("Should throw exception for null class")
    void shouldThrowExceptionForNullClass() {
        assertThrows(IllegalArgumentException.class, () ->
                EventUtils.fromJson("{}", null));
    }

    @Test
    @DisplayName("Should return configured ObjectMapper")
    void shouldReturnConfiguredObjectMapper() {
        ObjectMapper mapper = EventUtils.getObjectMapper();

        assertNotNull(mapper);
        assertNotNull(mapper.findAndRegisterModules());
    }

    static class SimpleEvent implements Event {
        public String id;
        public int value;

        SimpleEvent() {
        }

        SimpleEvent(String id, int value) {
            this.id = id;
            this.value = value;
        }

        @Override
        public Class<? extends Event> type() {
            return SimpleEvent.class;
        }
    }

    static class ComplexEvent implements Event {
        public String orderId;
        public List<String> items;
        public LocalDateTime timestamp;

        ComplexEvent() {
        }

        ComplexEvent(String orderId, List<String> items, LocalDateTime timestamp) {
            this.orderId = orderId;
            this.items = items;
            this.timestamp = timestamp;
        }

        @Override
        public Class<? extends Event> type() {
            return ComplexEvent.class;
        }
    }
}
