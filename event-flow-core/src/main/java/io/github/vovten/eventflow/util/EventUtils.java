package io.github.vovten.eventflow.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.EventLifecycle;
import io.github.vovten.eventflow.serialization.EventPolymorphicTypeValidator;
import io.github.vovten.eventflow.serialization.EventSerializationException;

/**
 * Utilities for working with events
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public final class EventUtils {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper()
                .disable(SerializationFeature.INDENT_OUTPUT)
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .setPolymorphicTypeValidator(new EventPolymorphicTypeValidator())
                .deactivateDefaultTyping();
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
     * Convert object to json string
     *
     * @param object the object
     * @return json string
     * @throws EventSerializationException if error occurs during object to json conversion
     */
    public static String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
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
     * Resolves the lifecycle for the given event.
     * <p>
     * This is the single canonical resolution method.
     * Resolution priority:
     * <ol>
     *   <li>{@link io.github.vovten.eventflow.event.annotation.Event @Event} annotation on the event class
     *       (or on the payload class for {@link Envelope})</li>
     *   <li>{@link Event#lifecycle()} default method (or {@link EventLifecycle#PERSISTED} for POJO payloads)</li>
     * </ol>
     *
     * @param event the event instance
     * @return the resolved lifecycle level
     */
    public static EventLifecycle lifecycle(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return resolveEnvelopeLifecycle(envelope);
        }
        var ann = event.getClass().getAnnotation(io.github.vovten.eventflow.event.annotation.Event.class);
        if (ann != null) {
            return ann.lifecycle();
        }
        return event.lifecycle();
    }

    private static EventLifecycle resolveEnvelopeLifecycle(Envelope<?> envelope) {
        Object payload = envelope.payload();
        var ann = payload.getClass().getAnnotation(io.github.vovten.eventflow.event.annotation.Event.class);
        if (ann != null) {
            return ann.lifecycle();
        }
        if (payload instanceof Event evt) {
            return evt.lifecycle();
        }
        return EventLifecycle.PERSISTED;
    }
}
