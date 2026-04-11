package io.github.vovten.eventflow.serialization;

import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for secure deserialization with type validation
 */
@DisplayName("Secure Deserialization Tests")
class SecureDeserializationTest {

    @AfterEach
    void tearDown() {
        EventTypeRegistry.clear();
    }

    @Test
    @DisplayName("Should deserialize allowed event successfully")
    void deserializesAllowedEventSuccessfully() {
        JsonEventSerializer serializer = new JsonEventSerializer();

        TestEvent event = new TestEvent("test-1", "Test message");
        byte[] serialized = serializer.serialize(event);

        // Should deserialize successfully (event-flow package is allowed by default)
        TestEvent deserialized = serializer.deserialize(serialized, TestEvent.class);

        assertEquals(event.getId(), deserialized.getId());
        assertEquals(event.getMessage(), deserialized.getMessage());
    }

    @Test
    @DisplayName("Should block malicious class deserialization")
    void blocksMaliciousClassDeserialization() {
        // Create a JSON payload with a malicious class name
        String maliciousJson = "{\"@class\":\"com.evil.MaliciousClass\",\"data\":\"hack\"}";
        byte[] maliciousData = maliciousJson.getBytes();

        // Prepend with magic byte for JSON
        byte[] dataWithMarker = new byte[maliciousData.length + 1];
        dataWithMarker[0] = 0x01; // Magic byte for JSON
        System.arraycopy(maliciousData, 0, dataWithMarker, 1, maliciousData.length);

        JsonEventSerializer serializer = new JsonEventSerializer();

        // Attempting to deserialize should fail because com.evil.MaliciousClass is not allowed
        // and is not an Event class
        assertThrows(EventSerializationException.class, () -> {
            serializer.deserialize(dataWithMarker, TestEvent.class);
        });
    }

    @Test
    @DisplayName("Should block non-Event class deserialization (defense-in-depth)")
    void blocksNonEventClassDeserialization() {
        // Try to deserialize a well-known non-Event class (defense-in-depth test)
        String nonEventJson = "{\"@class\":\"java.util.HashMap\",\"someField\":\"value\"}";
        byte[] data = nonEventJson.getBytes();

        byte[] dataWithMarker = new byte[data.length + 1];
        dataWithMarker[0] = 0x01;
        System.arraycopy(data, 0, dataWithMarker, 1, data.length);

        JsonEventSerializer serializer = new JsonEventSerializer();

        // Should be blocked because HashMap doesn't implement Event
        assertThrows(EventSerializationException.class, () -> {
            serializer.deserialize(dataWithMarker, TestEvent.class);
        });
    }

    @Test
    @DisplayName("Should allow explicitly registered package")
    void allowsExplicitlyRegisteredPackage() {
        // Allow external package
        EventTypeRegistry.allowPackage("io.github.vovten.eventflow.test");

        JsonEventSerializer serializer = new JsonEventSerializer();
        TestEvent event = new TestEvent("test-2", "Another test");
        byte[] serialized = serializer.serialize(event);

        TestEvent deserialized = serializer.deserialize(serialized, TestEvent.class);
        assertEquals(event.getId(), deserialized.getId());
    }
}
