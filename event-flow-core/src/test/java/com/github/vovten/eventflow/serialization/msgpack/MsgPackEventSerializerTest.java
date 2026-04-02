package com.github.vovten.eventflow.serialization.msgpack;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.EventSerializationException;
import com.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MsgPackEventSerializer.
 */
@DisplayName("MsgPackEventSerializer Tests")
class MsgPackEventSerializerTest {

    private final MsgPackEventSerializer serializer = new MsgPackEventSerializer();

    @Test
    @DisplayName("Should serialize event to byte array with magic byte")
    void shouldSerializeEventToByteArray() {
        SimpleEvent event = new SimpleEvent("test-id", 42);

        byte[] result = serializer.serialize(event);

        assertNotNull(result);
        assertTrue(result.length > 1);
        assertEquals(0x02, result[0]); // Magic byte for MessagePack
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
    @DisplayName("Should return msgpack format code")
    void shouldReturnMsgpackFormatCode() {
        assertEquals(0x02, serializer.getFormatCode());
    }

    @Test
    @DisplayName("Should return msgpack format name")
    void shouldReturnMsgpackFormatName() {
        assertEquals("msgpack", serializer.getFormat());
    }

    @Test
    @DisplayName("Should produce smaller output than JSON for simple event")
    void shouldProduceSmallerOutput() {
        SimpleEvent event = new SimpleEvent("test-id", 42);

        byte[] msgpackBytes = serializer.serialize(event);
        byte[] jsonBytes = new JsonEventSerializer().serialize(event);

        // MessagePack should be more compact
        assertTrue(msgpackBytes.length < jsonBytes.length,
                "MessagePack (" + msgpackBytes.length + " bytes) should be smaller than JSON (" + jsonBytes.length + " bytes)");
    }

    @Test
    @DisplayName("Should throw exception for null data")
    void shouldThrowExceptionForNullData() {
        assertThrows(EventSerializationException.class, () ->
                serializer.deserialize(null, SimpleEvent.class));
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
}
