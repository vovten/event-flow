package io.github.vovten.eventflow.serialization;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventSerializerFactory.
 */
@DisplayName("EventSerializerFactory Tests")
class EventSerializerFactoryTest {

    private EventSerializerFactory factory;

    @BeforeEach
    void setUp() {
        factory = new EventSerializerFactory();
    }

    @Test
    @DisplayName("Should get JSON serializer by code")
    void shouldGetJsonSerializerByCode() {
        EventSerializer serializer = factory.getByCode((byte) 0x01);

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
        assertEquals("json", serializer.getName());
    }

    @Test
    @DisplayName("Should get MessagePack serializer by code")
    void shouldGetMsgPackSerializerByCode() {
        EventSerializer serializer = factory.getByCode((byte) 0x02);

        assertNotNull(serializer);
        assertTrue(serializer instanceof MsgPackEventSerializer);
        assertEquals("msgpack", serializer.getName());
    }

    @Test
    @DisplayName("Should throw exception for unknown code")
    void shouldThrowExceptionForUnknownCode() {
        assertThrows(EventSerializationException.class, () ->
                factory.getByCode((byte) 0xFF));
    }

    @Test
    @DisplayName("Should get JSON serializer for old format data (without magic byte)")
    void shouldGetJsonSerializerForOldFormatData() {
        // Old JSON format: starts with '{' (0x7B)
        String oldJson = "{\"@class\":\"test.Event\",\"id\":\"test\"}";
        byte[] data = oldJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        EventSerializer serializer = factory.getByData(data);

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should get JSON serializer for new format data (with magic byte)")
    void shouldGetJsonSerializerForNewFormatData() {
        JsonEventSerializer jsonSerializer = new JsonEventSerializer();
        TestEvent event = new TestEvent("test");
        byte[] data = jsonSerializer.serialize(event);

        EventSerializer serializer = factory.getByData(data);

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should get MessagePack serializer for MessagePack data")
    void shouldGetMsgPackSerializerForMsgPackData() {
        MsgPackEventSerializer msgPackSerializer = new MsgPackEventSerializer();
        TestEvent event = new TestEvent("test");
        byte[] data = msgPackSerializer.serialize(event);

        EventSerializer serializer = factory.getByData(data);

        assertNotNull(serializer);
        assertTrue(serializer instanceof MsgPackEventSerializer);
    }

    @Test
    @DisplayName("Should throw exception for empty data")
    void shouldThrowExceptionForEmptyData() {
        assertThrows(EventSerializationException.class, () ->
                factory.getByData(new byte[0]));
    }

    @Test
    @DisplayName("Should throw exception for null data")
    void shouldThrowExceptionForNullData() {
        assertThrows(EventSerializationException.class, () ->
                factory.getByData(null));
    }

    @Test
    @DisplayName("Should get default JSON serializer")
    void shouldGetDefaultSerializer() {
        EventSerializer serializer = factory.getDefault();

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should get JSON serializer explicitly")
    void shouldGetJsonSerializer() {
        EventSerializer serializer = factory.getJson();

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should get MessagePack serializer explicitly")
    void shouldGetMsgPackSerializer() {
        EventSerializer serializer = factory.getMsgPack();

        assertNotNull(serializer);
        assertTrue(serializer instanceof MsgPackEventSerializer);
    }

    @Test
    @DisplayName("Should register custom serializer")
    void shouldRegisterCustomSerializer() {
        CustomEventSerializer customSerializer = new CustomEventSerializer();

        factory.register(customSerializer);

        EventSerializer retrievedByName = factory.getByName("custom");
        assertSame(customSerializer, retrievedByName);
        EventSerializer retrievedByCode = factory.getByCode((byte) 0x03);
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

        factory.register(overrideSerializer);

        EventSerializer retrieved = factory.getByName("json-override");
        assertSame(overrideSerializer, retrieved);
        EventSerializer byCode = factory.getByCode((byte) 0x01);
        assertSame(overrideSerializer, byCode);
    }

    @Test
    @DisplayName("Should throw exception when registering null serializer")
    void shouldThrowExceptionWhenRegisteringNullSerializer() {
        assertThrows(IllegalArgumentException.class, () ->
                factory.register(null));
    }

    @Test
    @DisplayName("Should return registered serializer names")
    void shouldReturnRegisteredSerializerNames() {
        var names = factory.getRegisteredNames();

        assertNotNull(names);
        assertTrue(names.contains("json"));
        assertTrue(names.contains("msgpack"));
        assertEquals(2, names.size());
    }

    @Test
    @DisplayName("Should return unmodifiable set of registered names")
    void shouldReturnUnmodifiableSetOfRegisteredNames() {
        var names = factory.getRegisteredNames();

        assertThrows(UnsupportedOperationException.class, () ->
                names.add("custom"));
    }

    @Test
    @DisplayName("Should include custom serializer in registered names")
    void shouldIncludeCustomSerializerInRegisteredNames() {
        factory.register(new CustomEventSerializer());

        var names = factory.getRegisteredNames();

        assertTrue(names.contains("custom"));
        assertEquals(3, names.size());
    }

    @Test
    @DisplayName("Should get serializer by name")
    void shouldGetSerializerByName() {
        EventSerializer serializer = factory.getByName("json");

        assertNotNull(serializer);
        assertTrue(serializer instanceof JsonEventSerializer);
    }

    @Test
    @DisplayName("Should throw exception for unknown serializer name")
    void shouldThrowExceptionForUnknownSerializerName() {
        assertThrows(EventSerializationException.class, () ->
                factory.getByName("unknown"));
    }

    @Test
    @DisplayName("Should clear and reset serializers to defaults")
    void shouldClearAndResetSerializersToDefaults() {
        // Register custom serializer
        factory.register(new CustomEventSerializer());

        // Create new factory instance to reset
        factory = new EventSerializerFactory();

        // Verify defaults are restored
        assertTrue(factory.getJson() instanceof JsonEventSerializer);
        assertTrue(factory.getMsgPack() instanceof MsgPackEventSerializer);
        assertEquals(2, factory.getRegisteredNames().size());
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
