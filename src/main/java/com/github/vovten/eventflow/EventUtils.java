// File: EventUtils.java
package com.github.vovten.eventflow;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Utilities for working with events
 *
 * @author Vladimir Aleshkov, 21.11.2024.
 */
public final class EventUtils {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper()
                .disable(SerializationFeature.INDENT_OUTPUT)
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    private EventUtils() {
    }

    /**
     * Convert event to json string
     *
     * @param event the event
     * @return json string
     * @throws EventSerializationException if error occurs during object to json conversion
     */
    public static String toJson(Event event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new EventSerializationException("Error converting object to json", e);
        }
    }

    /**
     * Convert json string to event
     *
     * @param json the json string
     * @param clazz the event class
     * @param <T> the event type
     * @return the event
     * @throws EventSerializationException if error occurs during json to object conversion
     */
    public static <T extends Event> T fromJson(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new EventSerializationException("Error converting json to object", e);
        }
    }

    /**
     * Get ObjectMapper for custom serialization/deserialization
     *
     * @return the ObjectMapper
     */
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
