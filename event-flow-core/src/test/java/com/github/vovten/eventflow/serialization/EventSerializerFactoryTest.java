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
    @DisplayName("Should get JSON serializer by code")
    void shouldGetJsonSerializerByCode() {
        EventSerializer serializer = EventSerializerFactory.getByCode((byte) 0x01);

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
        assertEquals("json", serializer.getName());
    }

    @Test
    @DisplayName("Should get MessagePack serializer by code")
    void shouldGetMsgPackSerializerByCode() {
        EventSerializer serializer = EventSerializerFactory.getByCode((byte) 0x02);

        assertNotNull(serializer);
        assertTrue(serializer instanceof MsgPackEventSerializer);
        assertEquals("msgpack", serializer.getName());
    }

    @Test
    @DisplayName("Should throw exception for unknown code")
    void shouldThrowExceptionForUnknownCode() {
        assertThrows(EventSerializationException.class, () ->
                EventSerializerFactory.getByCode((byte) 0xFF));
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

        EventSerializerFactory.register(customSerializer);

        EventSerializer retrievedByName = EventSerializerFactory.getByName("custom");
        assertSame(customSerializer, retrievedByName);
        EventSerializer retrievedByCode = EventSerializerFactory.getByCode((byte) 0x03);
        assertSame(customSerializer, retrievedByCode);
    }

    @Test
    @DisplayName("Should override existing serializer")
    void shouldOverrideExistingSerializer() {
        JsonEventSerializer overrideSerializer = new JsonEventSerializer() {
            @Override
            public String getName() {
                return "json-override";
            }

            @Override
            public byte getCode() {
                return 0x01; // Same code as JSON
            }
        };

        EventSerializerFactory.register(overrideSerializer);

        EventSerializer retrieved = EventSerializerFactory.getByName("json-override");
        assertSame(overrideSerializer, retrieved);
        EventSerializer byCode = EventSerializerFactory.getByCode((byte) 0x01);
        assertSame(overrideSerializer, byCode);
    }

    @Test
    @DisplayName("Should throw exception when registering null serializer")
    void shouldThrowExceptionWhenRegisteringNullSerializer() {
        assertThrows(IllegalArgumentException.class, () ->
                EventSerializerFactory.register(null));
    }

    @Test
    @DisplayName("Should return registered serializer names")
    void shouldReturnRegisteredSerializerNames() {
        var names = EventSerializerFactory.getRegisteredNames();

        assertNotNull(names);
        assertTrue(names.contains("json"));
        assertTrue(names.contains("msgpack"));
        assertEquals(2, names.size());
    }

    @Test
    @DisplayName("Should return unmodifiable set of registered names")
    void shouldReturnUnmodifiableSetOfRegisteredNames() {
        var names = EventSerializerFactory.getRegisteredNames();

        assertThrows(UnsupportedOperationException.class, () ->
                names.add("custom"));
    }

    @Test
    @DisplayName("Should include custom serializer in registered names")
    void shouldIncludeCustomSerializerInRegisteredNames() {
        EventSerializerFactory.register(new CustomEventSerializer());

        var names = EventSerializerFactory.getRegisteredNames();

        assertTrue(names.contains("custom"));
        assertEquals(3, names.size());
    }

    @Test
    @DisplayName("Should get serializer by name")
    void shouldGetSerializerByName() {
        EventSerializer serializer = EventSerializerFactory.getByName("json");

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should throw exception for unknown serializer name")
    void shouldThrowExceptionForUnknownSerializerName() {
        assertThrows(EventSerializationException.class, () ->
                EventSerializerFactory.getByName("unknown"));
    }

    @Test
    @DisplayName("Should clear and reset serializers to defaults")
    void shouldClearAndResetSerializersToDefaults() {
        // Register custom serializer
        EventSerializerFactory.register(new CustomEventSerializer());

        // Clear and reset
        EventSerializerFactory.clear();

        // Verify defaults are restored
        assertTrue(EventSerializerFactory.getJson() instanceof JsonEventSerializer);
        assertTrue(EventSerializerFactory.getMsgPack() instanceof MsgPackEventSerializer);
        assertEquals(2, EventSerializerFactory.getRegisteredNames().size());
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
        public byte getCode() {
            return 0x03;
        }

        @Override
        public String getName() {
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
