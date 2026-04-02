package com.github.vovten.eventflow.serialization.msgpack;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.EventSerializationException;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

/**
 * MessagePack-based event serializer using Jackson.
 * Simple and efficient binary serialization.
 * <p>
 * Format: [0x02][MessagePack bytes]
 * <p>
 * Uses ISO-8601 string format for Instants to preserve nanosecond precision.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
public class MsgPackEventSerializer implements EventSerializer {

    private static final byte FORMAT_CODE = 0x02;

    private final ObjectMapper mapper;

    public MsgPackEventSerializer() {
        this.mapper = new ObjectMapper(new MessagePackFactory())
                .registerModule(new JavaTimeModule()
                        .addSerializer(new InstantAsStringSerializer())
                );
    }

    @Override
    public byte[] serialize(Event event) {
        try {
            byte[] payload = mapper.writeValueAsBytes(event);
            return wrapWithFormat(payload);
        } catch (IOException e) {
            throw new EventSerializationException(
                    "Error serializing event " + event.type().getSimpleName() + " to MessagePack", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Event> T deserialize(byte[] data, Class<T> eventType) {
        try {
            byte[] payload = unwrapFormat(data);
            return (T) mapper.readValue(payload, eventType);
        } catch (IOException e) {
            throw new EventSerializationException(
                    "Error deserializing MessagePack to event " + eventType.getSimpleName(), e);
        }
    }

    private byte[] wrapWithFormat(byte[] payload) {
        byte[] result = new byte[payload.length + 1];
        result[0] = FORMAT_CODE;
        System.arraycopy(payload, 0, result, 1, payload.length);
        return result;
    }

    private byte[] unwrapFormat(byte[] data) {
        if (data == null || data.length == 0) {
            throw new EventSerializationException("Empty data");
        }

        if (data[0] != FORMAT_CODE) {
            throw new EventSerializationException(
                    "Expected format code " + FORMAT_CODE + " but got " + data[0]);
        }

        return Arrays.copyOfRange(data, 1, data.length);
    }

    @Override
    public byte getFormatCode() {
        return FORMAT_CODE;
    }

    @Override
    public String getFormat() {
        return "msgpack";
    }

    /**
     * Custom serializer that writes Instant as ISO-8601 string.
     */
    static class InstantAsStringSerializer extends StdSerializer<Instant> {
        InstantAsStringSerializer() {
            super(Instant.class);
        }

        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(value.toString());
        }
    }
}
