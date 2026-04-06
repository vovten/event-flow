package com.github.vovten.eventflow.serialization;

import com.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import com.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for obtaining event serializers by name or code.
 * <p>
 * Maintains two indexes for efficient lookups:
 * <ul>
 *   <li><b>By name</b> — used during serialization (configuration-driven)</li>
 *   <li><b>By code</b> — used during deserialization (magic byte detection)</li>
 * </ul>
 * <p>
 * Automatically detects serializer format from data bytes:
 * - 0x7B ('{'): JSON (backward compatible, no magic byte)
 * - 0x01: JSON (new format with magic byte)
 * - 0x02: MessagePack
 * <p>
 * Custom serializers can be registered using {@link #register(EventSerializer)}.
 * The name and code are taken automatically from the serializer itself.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
@Slf4j
public final class EventSerializerFactory {

    private static final byte JSON_START_BYTE = 0x7B;

    private static final Map<String, EventSerializer> BY_NAME = new ConcurrentHashMap<>();
    private static final Map<Byte, EventSerializer> BY_CODE = new ConcurrentHashMap<>();

    static {
        register(new JsonEventSerializer());
        register(new MsgPackEventSerializer());
    }

    private EventSerializerFactory() {
        // Utility class
    }

    /**
     * Register a custom serializer.
     * <p>
     * The serializer's {@link EventSerializer#getName()} and {@link EventSerializer#getCode()}
     * are used to populate both name and code indexes automatically.
     * <p>
     * <b>Examples:</b>
     * <pre>{@code
     * // Override default MessagePack serializer
     * EventSerializerFactory.register(new CustomMsgPackSerializer());
     *
     * // Register a new Protobuf serializer
     * EventSerializerFactory.register(new ProtobufEventSerializer());
     * }</pre>
     *
     * @param serializer the serializer instance
     * @throws IllegalArgumentException if serializer is null
     */
    public static void register(EventSerializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer must not be null");
        }
        String name = serializer.getName();
        byte code = serializer.getCode();

        if (BY_NAME.containsKey(name) || BY_CODE.containsKey(code)) {
            log.warn("Overwriting existing serializer: name='{}', code=0x{}",
                    name, Integer.toHexString(code & 0xFF));
        }
        BY_NAME.put(name, serializer);
        BY_CODE.put(code, serializer);
    }

    /**
     * Get serializer by name.
     * <p>
     * Used during serialization when format is specified in configuration.
     *
     * @param name the serializer name (e.g., "json", "msgpack", "protobuf")
     * @return the serializer
     * @throws EventSerializationException if name is unknown
     */
    public static EventSerializer getByName(String name) {
        EventSerializer serializer = BY_NAME.get(name);
        if (serializer == null) {
            throw new EventSerializationException("Unknown serializer name: " + name);
        }
        return serializer;
    }

    /**
     * Get serializer by code.
     * <p>
     * Used during deserialization to resolve serializer from the magic byte.
     *
     * @param code the serializer code (magic byte)
     * @return the serializer
     * @throws EventSerializationException if code is unknown
     */
    public static EventSerializer getByCode(byte code) {
        EventSerializer serializer = BY_CODE.get(code);
        if (serializer == null) {
            throw new EventSerializationException("Unknown serializer code: " + code);
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
            return BY_NAME.get("json");
        }
        // New format with magic byte
        return getByCode(firstByte);
    }

    /**
     * Get default JSON serializer.
     *
     * @return JSON serializer
     */
    public static EventSerializer getDefault() {
        return BY_NAME.get("json");
    }

    /**
     * Get JSON serializer.
     *
     * @return JSON serializer
     */
    public static EventSerializer getJson() {
        return BY_NAME.get("json");
    }

    /**
     * Get MessagePack serializer.
     *
     * @return MessagePack serializer
     */
    public static EventSerializer getMsgPack() {
        return BY_NAME.get("msgpack");
    }

    /**
     * Get a set of all registered serializer names.
     * <p>
     * Useful for debugging and inspection.
     *
     * @return unmodifiable set of registered serializer names
     */
    public static Set<String> getRegisteredNames() {
        return Collections.unmodifiableSet(BY_NAME.keySet());
    }

    /**
     * Get a set of all registered serializer codes.
     * <p>
     * Useful for debugging and inspection.
     *
     * @return unmodifiable set of registered serializer codes
     */
    public static Set<Byte> getRegisteredCodes() {
        return Collections.unmodifiableSet(BY_CODE.keySet());
    }

    /**
     * Clear all registered serializers and reset to defaults.
     * <p>
     * Package-private method intended for testing purposes only.
     * Use with caution as it affects global state.
     */
    static void clear() {
        BY_NAME.clear();
        BY_CODE.clear();
        register(new JsonEventSerializer());
        register(new MsgPackEventSerializer());
    }
}
