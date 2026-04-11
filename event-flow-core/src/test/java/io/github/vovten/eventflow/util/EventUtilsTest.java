package io.github.vovten.eventflow.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
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
        ComplexEvent event = new ComplexEvent("order-123", List.of("item1", "item2"), Instant.ofEpochSecond(1704103200));

        String json = EventUtils.toJson(event);

        assertNotNull(json);
        assertTrue(json.contains("order-123"));
        assertTrue(json.contains("item1"));
        assertTrue(json.contains("item2"));
    }

    @Test
    @DisplayName("Should convert JSON to event")
    void shouldConvertJsonToEvent() {
        String json = "{\"id\":\"test-id\",\"value\":42,\"@class\":\"io.github.vovten.eventflow.util.EventUtilsTest$SimpleEvent\"}";

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

    static class SimpleEvent extends AbstractTraceableEvent {
        public String id;
        public int value;

        SimpleEvent() {
            super();
        }

        SimpleEvent(String id, int value) {
            super();
            this.id = id;
            this.value = value;
        }

        @Override
        public Class<? extends Event> type() {
            return SimpleEvent.class;
        }
    }

    static class ComplexEvent extends AbstractTraceableEvent {
        public String orderId;
        public List<String> items;
        public Instant timestamp;

        ComplexEvent() {
            super();
        }

        ComplexEvent(String orderId, List<String> items, Instant timestamp) {
            super();
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
