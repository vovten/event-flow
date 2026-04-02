package com.github.vovten.eventflow.serialization;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import com.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventSerializerFactory.
 */
@DisplayName("EventSerializerFactory Tests")
class EventSerializerFactoryTest {

    @AfterEach
    void tearDown() {
        // Reset factory to default state after each test
        EventSerializerFactory.clear();
    }

    @Test
    @DisplayName("Should get JSON serializer by format code")
    void shouldGetJsonSerializerByFormatCode() {
        EventSerializer serializer = EventSerializerFactory.getByFormatCode((byte) 0x01);

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
        assertEquals("json", serializer.getFormat());
    }

    @Test
    @DisplayName("Should get MessagePack serializer by format code")
    void shouldGetMsgPackSerializerByFormatCode() {
        EventSerializer serializer = EventSerializerFactory.getByFormatCode((byte) 0x02);

        assertNotNull(serializer);
        assertTrue(serializer instanceof MsgPackEventSerializer);
        assertEquals("msgpack", serializer.getFormat());
    }

    @Test
    @DisplayName("Should throw exception for unknown format code")
    void shouldThrowExceptionForUnknownFormatCode() {
        assertThrows(EventSerializationException.class, () ->
                EventSerializerFactory.getByFormatCode((byte) 0xFF));
    }

    @Test
    @DisplayName("Should get JSON serializer for old format data (without magic byte)")
    void shouldGetJsonSerializerForOldFormatData() {
        // Old JSON format: starts with '{' (0x7B)
        String oldJson = "{\"@class\":\"test.Event\",\"id\":\"test\"}";
        byte[] data = oldJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        EventSerializer serializer = EventSerializerFactory.getByData(data);

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should get JSON serializer for new format data (with magic byte)")
    void shouldGetJsonSerializerForNewFormatData() {
        JsonEventSerializer jsonSerializer = new JsonEventSerializer();
        TestEvent event = new TestEvent("test");
        byte[] data = jsonSerializer.serialize(event);

        EventSerializer serializer = EventSerializerFactory.getByData(data);

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should get MessagePack serializer for MessagePack data")
    void shouldGetMsgPackSerializerForMsgPackData() {
        MsgPackEventSerializer msgPackSerializer = new MsgPackEventSerializer();
        TestEvent event = new TestEvent("test");
        byte[] data = msgPackSerializer.serialize(event);

        EventSerializer serializer = EventSerializerFactory.getByData(data);

        assertNotNull(serializer);
        assertTrue(serializer instanceof MsgPackEventSerializer);
    }

    @Test
    @DisplayName("Should throw exception for empty data")
    void shouldThrowExceptionForEmptyData() {
        assertThrows(EventSerializationException.class, () ->
                EventSerializerFactory.getByData(new byte[0]));
    }

    @Test
    @DisplayName("Should throw exception for null data")
    void shouldThrowExceptionForNullData() {
        assertThrows(EventSerializationException.class, () ->
                EventSerializerFactory.getByData(null));
    }

    @Test
    @DisplayName("Should get default JSON serializer")
    void shouldGetDefaultSerializer() {
        EventSerializer serializer = EventSerializerFactory.getDefault();

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should get JSON serializer explicitly")
    void shouldGetJsonSerializer() {
        EventSerializer serializer = EventSerializerFactory.getJson();

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should get MessagePack serializer explicitly")
    void shouldGetMsgPackSerializer() {
        EventSerializer serializer = EventSerializerFactory.getMsgPack();

        assertNotNull(serializer);
        assertTrue(serializer instanceof MsgPackEventSerializer);
    }

    @Test
    @DisplayName("Should register custom serializer")
    void shouldRegisterCustomSerializer() {
        CustomEventSerializer customSerializer = new CustomEventSerializer();

        EventSerializerFactory.register((byte) 0x03, customSerializer);

        EventSerializer retrieved = EventSerializerFactory.getByFormatCode((byte) 0x03);
        assertSame(customSerializer, retrieved);
    }

    @Test
    @DisplayName("Should override existing serializer")
    void shouldOverrideExistingSerializer() {
        CustomEventSerializer customSerializer = new CustomEventSerializer();

        EventSerializerFactory.register((byte) 0x01, customSerializer);

        EventSerializer retrieved = EventSerializerFactory.getByFormatCode((byte) 0x01);
        assertSame(customSerializer, retrieved);
        assertFalse(retrieved instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should throw exception when registering null serializer")
    void shouldThrowExceptionWhenRegisteringNullSerializer() {
        assertThrows(IllegalArgumentException.class, () ->
                EventSerializerFactory.register((byte) 0x04, null));
    }

    @Test
    @DisplayName("Should return registered format codes")
    void shouldReturnRegisteredFormatCodes() {
        var formatCodes = EventSerializerFactory.getRegisteredFormatCodes();

        assertNotNull(formatCodes);
        assertTrue(formatCodes.contains((byte) 0x01)); // JSON
        assertTrue(formatCodes.contains((byte) 0x02)); // MessagePack
        assertEquals(2, formatCodes.size());
    }

    @Test
    @DisplayName("Should return unmodifiable set of format codes")
    void shouldReturnUnmodifiableSetOfFormatCodes() {
        var formatCodes = EventSerializerFactory.getRegisteredFormatCodes();

        assertThrows(UnsupportedOperationException.class, () ->
                formatCodes.add((byte) 0x05));
    }

    @Test
    @DisplayName("Should include custom serializer in registered format codes")
    void shouldIncludeCustomSerializerInRegisteredFormatCodes() {
        EventSerializerFactory.register((byte) 0x03, new CustomEventSerializer());

        var formatCodes = EventSerializerFactory.getRegisteredFormatCodes();

        assertTrue(formatCodes.contains((byte) 0x03));
        assertEquals(3, formatCodes.size());
    }

    @Test
    @DisplayName("Should clear and reset serializers to defaults")
    void shouldClearAndResetSerializersToDefaults() {
        // Register custom serializer
        EventSerializerFactory.register((byte) 0x03, new CustomEventSerializer());
        EventSerializerFactory.register((byte) 0x01, new CustomEventSerializer());

        // Clear and reset
        EventSerializerFactory.clear();

        // Verify defaults are restored
        assertTrue(EventSerializerFactory.getJson() instanceof JsonEventSerializer);
        assertTrue(EventSerializerFactory.getMsgPack() instanceof MsgPackEventSerializer);
        assertEquals(2, EventSerializerFactory.getRegisteredFormatCodes().size());
    }

    /**
     * Custom test serializer.
     */
    static class CustomEventSerializer implements EventSerializer {
        @Override
        public byte[] serialize(Event event) {
            return new byte[0];
        }

        @Override
        public <T extends Event> T deserialize(byte[] data, Class<T> eventType) {
            return null;
        }

        @Override
        public byte getFormatCode() {
            return 0;
        }

        @Override
        public String getFormat() {
            return "custom";
        }
    }

    static class TestEvent extends AbstractTraceableEvent {
        public String data;

        TestEvent() {
            super();
        }

        TestEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
