package com.github.vovten.eventflow.serialization;

import com.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import com.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for obtaining event serializers by format code.
 * <p>
 * Automatically detects serializer format from data bytes:
 * - 0x7B ('{'): JSON (backward compatible, no magic byte)
 * - 0x01: JSON (new format with magic byte)
 * - 0x02: MessagePack
 * <p>
 * Custom serializers can be registered using {@link #register(byte, EventSerializer)}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
public final class EventSerializerFactory {

    private static final byte JSON_START_BYTE = 0x7B;
    private static final byte JSON_FORMAT_CODE = 0x01;
    private static final byte MSGPACK_FORMAT_CODE = 0x02;

    private static final Map<Byte, EventSerializer> SERIALIZERS = new ConcurrentHashMap<>();

    static {
        SERIALIZERS.put(JSON_FORMAT_CODE, new JsonEventSerializer());
        SERIALIZERS.put(MSGPACK_FORMAT_CODE, new MsgPackEventSerializer());
    }

    private EventSerializerFactory() {
        // Utility class
    }

    /**
     * Register a custom serializer for the specified format code.
     * <p>
     * This method allows overriding default serializers or registering new ones.
     * <p>
     * <b>Examples:</b>
     * <pre>{@code
     * // Override default MessagePack serializer
     * EventSerializerFactory.register((byte) 0x02, new CustomMsgPackSerializer());
     *
     * // Register a new Protobuf serializer
     * EventSerializerFactory.register((byte) 0x03, new ProtobufEventSerializer());
     * }</pre>
     *
     * @param formatCode the format code (magic byte)
     * @param serializer the serializer instance
     * @throws IllegalArgumentException if serializer is null
     */
    public static void register(byte formatCode, EventSerializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer must not be null");
        }
        SERIALIZERS.put(formatCode, serializer);
    }

    /**
     * Get serializer by format code.
     *
     * @param formatCode the format code (magic byte)
     * @return the serializer
     * @throws EventSerializationException if format code is unknown
     */
    public static EventSerializer getByFormatCode(byte formatCode) {
        EventSerializer serializer = SERIALIZERS.get(formatCode);
        if (serializer == null) {
            throw new EventSerializationException("Unknown format code: " + formatCode);
        }
        return serializer;
    }

    /**
     * Get serializer by examining data bytes.
     * <p>
     * Automatically detects format:
     * - If first byte is 0x7B ('{'): JSON (old format without magic byte)
     * - If first byte is 0x01: JSON (new format)
     * - If first byte is 0x02: MessagePack
     *
     * @param data the serialized data
     * @return the appropriate serializer
     * @throws EventSerializationException if data is empty or format is unknown
     */
    public static EventSerializer getByData(byte[] data) {
        if (data == null || data.length == 0) {
            throw new EventSerializationException("Empty data");
        }

        byte firstByte = data[0];

        // Backward compatibility: old JSON format without magic byte
        if (firstByte == JSON_START_BYTE) {
            return SERIALIZERS.get(JSON_FORMAT_CODE);
        }

        // New format with magic byte
        return getByFormatCode(firstByte);
    }

    /**
     * Get default JSON serializer.
     *
     * @return JSON serializer
     */
    public static EventSerializer getDefault() {
        return SERIALIZERS.get(JSON_FORMAT_CODE);
    }

    /**
     * Get JSON serializer.
     *
     * @return JSON serializer
     */
    public static EventSerializer getJson() {
        return SERIALIZERS.get(JSON_FORMAT_CODE);
    }

    /**
     * Get MessagePack serializer.
     *
     * @return MessagePack serializer
     */
    public static EventSerializer getMsgPack() {
        return SERIALIZERS.get(MSGPACK_FORMAT_CODE);
    }

    /**
     * Get a set of all registered format codes.
     * <p>
     * Useful for debugging and inspection.
     *
     * @return unmodifiable set of registered format codes
     */
    public static Set<Byte> getRegisteredFormatCodes() {
        return Collections.unmodifiableSet(SERIALIZERS.keySet());
    }

    /**
     * Clear all registered serializers and reset to defaults.
     * <p>
     * Package-private method intended for testing purposes only.
     * Use with caution as it affects global state.
     */
    static void clear() {
        SERIALIZERS.clear();
        SERIALIZERS.put(JSON_FORMAT_CODE, new JsonEventSerializer());
        SERIALIZERS.put(MSGPACK_FORMAT_CODE, new MsgPackEventSerializer());
    }
}
