package com.github.vovten.eventflow.serialization.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.EventPolymorphicTypeValidator;
import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.EventSerializationException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * JSON-based event serializer using Jackson.
 * <p>
 * Format: [0x01][JSON UTF-8 bytes]
 * <p>
 * JSON is serialized to String then converted to UTF-8 bytes.
 * This allows viewing JSON messages as text in Kafka UI tools
 * that support UTF-8 decoding (e.g., Kafka Tool, AKHQ, Confluent Control Center).
 * <p>
 * Supports backward compatibility: can deserialize old JSON format without magic byte
 * (detects by first byte = 0x7B = '{').
 * <p>
 * Uses {@link EventPolymorphicTypeValidator} to prevent deserialization attacks
 * by validating class names against a whitelist of allowed packages/classes.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
public class JsonEventSerializer implements EventSerializer {

    private static final byte FORMAT_CODE = 0x01;
    private static final byte JSON_START_BYTE = 0x7B; // '{'

    private final ObjectMapper objectMapper;

    public JsonEventSerializer() {
        this.objectMapper = new ObjectMapper()
                .disable(SerializationFeature.INDENT_OUTPUT)
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .setPolymorphicTypeValidator(new EventPolymorphicTypeValidator())
                .deactivateDefaultTyping();
    }

    @Override
    public byte[] serialize(Event event) {
        try {
            // Serialize to JSON String then convert to UTF-8 bytes
            // This allows viewing messages as text in Kafka UI tools
            String json = objectMapper.writeValueAsString(event);
            byte[] payload = json.getBytes(StandardCharsets.UTF_8);
            return wrapWithFormat(payload);
        } catch (JsonProcessingException e) {
            throw new EventSerializationException(
                    "Error serializing event " + event.type().getSimpleName() + " to JSON", e);
        }
    }

    @Override
    public <T extends Event> T deserialize(byte[] data, Class<T> eventType) {
        try {
            byte[] payload = unwrapFormat(data);
            return objectMapper.readValue(payload, eventType);
        } catch (java.io.IOException e) {
            throw new EventSerializationException(
                    "Error deserializing JSON to event " + eventType.getSimpleName(), e);
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

        // Backward compatibility: old JSON format without magic byte
        if (data[0] == JSON_START_BYTE) {
            return data;
        }

        if (data[0] != FORMAT_CODE) {
            throw new EventSerializationException(
                    "Expected format code " + FORMAT_CODE + " or JSON start byte " + JSON_START_BYTE
                            + " but got " + data[0]);
        }

        return Arrays.copyOfRange(data, 1, data.length);
    }

    @Override
    public byte getCode() {
        return FORMAT_CODE;
    }

    @Override
    public String getName() {
        return "json";
    }
}
