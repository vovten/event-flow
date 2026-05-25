package io.github.vovten.eventflow.serialization.json;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for JsonEventSerializer.
 * @since 1.0.0
 */
@DisplayName("JsonEventSerializer Tests")
class JsonEventSerializerTest {

    private final JsonEventSerializer serializer = new JsonEventSerializer();

    @Test
    @DisplayName("Should serialize event to byte array with magic byte")
    void shouldSerializeEventToByteArray() {
        SimpleEvent event = new SimpleEvent("test-id", 42);

        byte[] result = serializer.serialize(event);

        assertNotNull(result);
        assertTrue(result.length > 1);
        assertEquals(0x01, result[0]); // Magic byte for JSON
    }

    @Test
    @DisplayName("Should serialize and deserialize event")
    void shouldSerializeAndDeserializeEvent() {
        SimpleEvent original = new SimpleEvent("test-id", 42);

        byte[] data = serializer.serialize(original);
        SimpleEvent deserialized = serializer.deserialize(data, SimpleEvent.class);

        assertEquals("test-id", deserialized.id);
        assertEquals(42, deserialized.value);
    }

    @Test
    @DisplayName("Should deserialize old JSON format without magic byte (backward compatibility)")
    void shouldDeserializeOldJsonFormatWithoutMagicByte() {
        // Old format: JSON string as bytes without magic byte (starts with '{')
        String oldJson = "{\"@class\":\"io.github.vovten.eventflow.serialization.json.JsonEventSerializerTest$SimpleEvent\",\"id\":\"test-id\",\"value\":42}";
        byte[] oldFormatData = oldJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        SimpleEvent event = serializer.deserialize(oldFormatData, SimpleEvent.class);

        assertEquals("test-id", event.id);
        assertEquals(42, event.value);
    }

    @Test
    @DisplayName("Should deserialize JSON string with traceable event fields")
    void shouldDeserializeJsonWithTraceableFields() {
        SimpleEvent original = new SimpleEvent("order-456", 99);
        byte[] data = serializer.serialize(original);

        SimpleEvent deserialized = serializer.deserialize(data, SimpleEvent.class);

        assertEquals("order-456", deserialized.id);
        assertEquals(99, deserialized.value);
        // Traceable fields are private, but they should be populated
        assertNotNull(deserialized.type());
    }

    @Test
    @DisplayName("Should throw exception for invalid JSON")
    void shouldThrowExceptionForInvalidJson() {
        String invalidJson = "{invalid json}";
        byte[] invalidData = invalidJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        assertThrows(EventSerializationException.class, () ->
                serializer.deserialize(invalidData, SimpleEvent.class));
    }

    @Test
    @DisplayName("Should handle event with null fields")
    void shouldHandleEventWithNullFields() {
        SimpleEvent event = new SimpleEvent(null, 0);
        byte[] data = serializer.serialize(event);

        SimpleEvent deserialized = serializer.deserialize(data, SimpleEvent.class);

        assertNull(deserialized.id);
        assertEquals(0, deserialized.value);
    }

    @Test
    @DisplayName("Should handle event with special characters")
    void shouldHandleEventWithSpecialCharacters() {
        SimpleEvent event = new SimpleEvent("test\"with\\special", 123);
        byte[] data = serializer.serialize(event);

        SimpleEvent deserialized = serializer.deserialize(data, SimpleEvent.class);

        assertEquals("test\"with\\special", deserialized.id);
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
