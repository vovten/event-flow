package com.github.vovten.eventflow.serialization;

import com.github.vovten.eventflow.event.Event;

/**
 * Contract for event serialization and deserialization.
 * <p>
 * All implementations serialize to byte[] format with a magic byte prefix:
 * - First byte: format code (magic byte)
 * - Remaining bytes: serialized event data
 * <p>
 * Format codes:
 * - 0x01: JSON
 * - 0x02: MessagePack
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
public interface EventSerializer {

    /**
     * Serialize event to byte array with format header.
     * <p>
     * Format: [1 byte format code][serialized data]
     *
     * @param event the event to serialize
     * @return serialized event as byte array
     * @throws EventSerializationException if serialization fails
     */
    byte[] serialize(Event event);

    /**
     * Deserialize event from byte array with format header.
     * <p>
     * Automatically detects format from first byte.
     *
     * @param data      the serialized data with format header
     * @param eventType the event class type
     * @param <T>       the event type
     * @return the deserialized event
     * @throws EventSerializationException if deserialization fails
     */
    <T extends Event> T deserialize(byte[] data, Class<T> eventType);

    /**
     * Get the format code (magic byte) of this serializer.
     *
     * @return format code (1 byte)
     */
    byte getFormatCode();

    /**
     * Get the format name for logging/configuration.
     *
     * @return format name (e.g., "json", "msgpack")
     */
    String getFormat();
}
