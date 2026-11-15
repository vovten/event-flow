package io.github.vovten.eventflow.serialization;

import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
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
 * Registering a name or code that is already in use is rejected; replacing a
 * built-in serializer requires an explicit
 * {@link #register(EventSerializer, boolean) register(serializer, true)}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class EventSerializerFactory {

    private static final Logger log = LoggerFactory.getLogger(EventSerializerFactory.class);

    private static final byte JSON_START_BYTE = 0x7B;

    /**
     * Built-in JSON format code. Legacy data (without a magic byte) is decoded by
     * this code, never by the {@code "json"} name: a replacement serializer can
     * be registered under a different name.
     */
    private static final byte JSON_CODE = 0x01;

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
     * Registration fails if the name or the code is already in use; use
     * {@link #register(EventSerializer, boolean)} with {@code overwrite=true} to
     * replace an existing serializer explicitly.
     * <p>
     * <b>Examples:</b>
     * <pre>{@code
     * // Register a new Protobuf serializer
     * registry.register(new ProtobufEventSerializer());
     *
     * // Explicitly replace the default JSON serializer
     * registry.register(new CustomJsonSerializer(), true);
     * }</pre>
     *
     * @param serializer the serializer instance
     * @throws IllegalArgumentException if serializer is null or its name is blank
     * @throws EventSerializationException if the name or code is already registered
     */
    public void register(EventSerializer serializer) {
        register(serializer, false);
    }

    /**
     * Register a custom serializer, optionally replacing an existing one.
     * <p>
     * Without {@code overwrite}, a name or code conflict is rejected so that a
     * custom serializer can never silently replace a built-in format (which would
     * break deserialization for every component and for legacy payloads).
     * <p>
     * With {@code overwrite}, the new serializer takes over the keys of the
     * replaced one: it is registered under its own name and code <b>and</b> under
     * the previous serializer's name and code. This keeps every lookup path
     * consistent — for example, a custom serializer registered with code
     * {@code 0x01} and a different name still answers the {@code "json"} name and
     * legacy {@code 0x7B} payloads.
     *
     * @param serializer the serializer instance
     * @param overwrite  whether an existing serializer with the same name or code
     *                   may be replaced
     * @throws IllegalArgumentException if serializer is null or its name is blank
     * @throws EventSerializationException if the name or code is already registered
     *                                     and {@code overwrite} is false
     */
    public void register(EventSerializer serializer, boolean overwrite) {
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer must not be null");
        }
        String name = serializer.getName();
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Serializer name must not be blank");
        }
        byte code = serializer.getCode();

        EventSerializer existingByName = byName.get(name);
        EventSerializer existingByCode = byCode.get(code);
        if (existingByName == serializer && existingByCode == serializer) {
            // Registering the very same instance again is idempotent.
            return;
        }
        if ((existingByName != null || existingByCode != null) && !overwrite) {
            throw new EventSerializationException(
                    "Serializer name '" + name + "' or code 0x"
                            + Integer.toHexString(code & 0xFF)
                            + " is already registered; use register(serializer, true) to replace it");
        }
        if (!overwrite) {
            byName.put(name, serializer);
            byCode.put(code, serializer);
            return;
        }
        Set<String> names = new LinkedHashSet<>();
        Set<Byte> codes = new LinkedHashSet<>();
        takeOver(existingByName, names, codes);
        takeOver(existingByCode, names, codes);
        names.add(name);
        codes.add(code);
        names.forEach(alias -> byName.put(alias, serializer));
        codes.forEach(alias -> byCode.put(alias, serializer));
        log.info("Replaced serializer: name='{}', code=0x{}",
                name, Integer.toHexString(code & 0xFF));
    }

    private void takeOver(EventSerializer existing, Set<String> names, Set<Byte> codes) {
        if (existing != null) {
            names.add(existing.getName());
            codes.add(existing.getCode());
            removeSerializer(existing);
        }
    }

    private void removeSerializer(EventSerializer existing) {
        if (existing != null) {
            byName.remove(existing.getName(), existing);
            byCode.remove(existing.getCode(), existing);
        }
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
        // Backward compatibility: old JSON format without magic byte. Route by
        // the JSON format code, never by the "json" name: a custom serializer
        // could be registered under that name with a different code.
        if (firstByte == JSON_START_BYTE) {
            return getByCode(JSON_CODE);
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
