package com.github.vovten.eventflow.serialization;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import com.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventSerializerFactory.
 */
@DisplayName("EventSerializerFactory Tests")
class EventSerializerFactoryTest {

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
