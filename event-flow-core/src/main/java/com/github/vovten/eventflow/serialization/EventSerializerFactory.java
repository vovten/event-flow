package com.github.vovten.eventflow.serialization;

import com.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import com.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for obtaining event serializers by name or code.
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
public class EventSerializerFactory {

    private static final byte JSON_START_BYTE = 0x7B;

    private final Map<String, EventSerializer> byName = new ConcurrentHashMap<>();
    private final Map<Byte, EventSerializer> byCode = new ConcurrentHashMap<>();

    /**
     * Creates a new registry with default serializers (JSON and MessagePack).
     */
    public EventSerializerFactory() {
        register(new JsonEventSerializer());
        register(new MsgPackEventSerializer());
    }

    /**
     * Creates a new empty registry (for tests or custom setup).
     *
     * @param withDefaults if true, registers JSON and MessagePack serializers
     */
    public EventSerializerFactory(boolean withDefaults) {
        if (withDefaults) {
            register(new JsonEventSerializer());
            register(new MsgPackEventSerializer());
        }
    }

    /**
     * Register a custom serializer.
     * <p>
     * <b>Examples:</b>
     * <pre>{@code
     * // Override default MessagePack serializer
     * registry.register(new CustomMsgPackSerializer());
     *
     * // Register a new Protobuf serializer
     * registry.register(new ProtobufEventSerializer());
     * }</pre>
     *
     * @param serializer the serializer instance
     * @throws IllegalArgumentException if serializer is null
     */
    public void register(EventSerializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer must not be null");
        }
        String name = serializer.getName();
        byte code = serializer.getCode();

        if (byName.containsKey(name) || byCode.containsKey(code)) {
            log.warn("Overwriting existing serializer: name='{}', code=0x{}",
                    name, Integer.toHexString(code & 0xFF));
        }
        byName.put(name, serializer);
        byCode.put(code, serializer);
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
    public EventSerializer getByName(String name) {
        EventSerializer serializer = byName.get(name);
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
    public EventSerializer getByCode(byte code) {
        EventSerializer serializer = byCode.get(code);
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
    public EventSerializer getByData(byte[] data) {
        if (data == null || data.length == 0) {
            throw new EventSerializationException("Empty data");
        }
        byte firstByte = data[0];
        // Backward compatibility: old JSON format without magic byte
        if (firstByte == JSON_START_BYTE) {
            return byName.get("json");
        }
        // New format with magic byte
        return getByCode(firstByte);
    }

    /**
     * Get default JSON serializer.
     *
     * @return JSON serializer
     */
    public EventSerializer getDefault() {
        return byName.get("json");
    }

    /**
     * Get JSON serializer.
     *
     * @return JSON serializer
     */
    public EventSerializer getJson() {
        return byName.get("json");
    }

    /**
     * Get MessagePack serializer.
     *
     * @return MessagePack serializer
     */
    public EventSerializer getMsgPack() {
        return byName.get("msgpack");
    }

    /**
     * Get a set of all registered serializer names.
     * <p>
     * Useful for debugging and inspection.
     *
     * @return unmodifiable set of registered serializer names
     */
    public Set<String> getRegisteredNames() {
        return Collections.unmodifiableSet(byName.keySet());
    }

    /**
     * Get a set of all registered serializer codes.
     * <p>
     * Useful for debugging and inspection.
     *
     * @return unmodifiable set of registered serializer codes
     */
    public Set<Byte> getRegisteredCodes() {
        return Collections.unmodifiableSet(byCode.keySet());
    }
}
