package io.github.vovten.eventflow.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.util.EventUtils;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Envelope wrapper for events that adds technical metadata.
 * <p>
 * Automatically captures: eventId (UUID), occurredAt (Instant).
 * processId is optional and can be set via factory methods or builder.
 * Additional metadata can be added via {@link #metadata()}.
 * <p>
 * The envelope implements {@link Event} interface, so it passes through
 * existing transport infrastructure without modifications.
 *
 * @param <T> the type of the wrapped payload
 * @author Vladimir Aleshkov
 * @since 2026-05-03
 */
public final class Envelope<T> implements TraceableEvent {

    private final UUID eventId;
    private final UUID processId;
    private final Instant occurredAt;
    private final Map<String, String> metadata;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final T payload;

    private final transient List<Class<? extends EventChannel>> targetChannels;

    @JsonCreator
    public Envelope(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("processId") UUID processId,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("payload") T payload,
            @JsonProperty("metadata") Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.processId = processId;
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.metadata = Map.copyOf(metadata);
        this.targetChannels = null;
    }

    Envelope(
            UUID eventId,
            UUID processId,
            Instant occurredAt,
            T payload,
            Map<String, String> metadata,
            List<Class<? extends EventChannel>> targetChannels) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.processId = processId;
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.metadata = Map.copyOf(metadata);
        this.targetChannels = targetChannels;
    }

    /**
     * Create envelope with auto-generated eventId, null processId, and current timestamp.
     * Channels are resolved from payload's {@link Event} annotation, or default to internal.
     *
     * @param <T>     the payload type
     * @param payload the event to wrap
     * @return new envelope instance
     */
    public static <T> Envelope<T> of(T payload) {
        return new Envelope<>(
                UUID.randomUUID(),
                null,
                Instant.now(),
                payload,
                Map.of(),
                null
        );
    }

    /**
     * Create envelope with auto-generated eventId, specified processId, and current timestamp.
     * Channels are resolved from payload's {@link Event} annotation, or default to internal.
     *
     * @param <T>        the payload type
     * @param payload    the domain event to wrap
     * @param processId  the process identifier (e.g., saga ID)
     * @return new envelope instance
     */
    public static <T> Envelope<T> of(T payload, UUID processId) {
        return new Envelope<>(
                UUID.randomUUID(),
                processId,
                Instant.now(),
                payload,
                Map.of(),
                null
        );
    }

    /**
     * Create envelope with auto-generated eventId, null processId, and specified channels.
     * Channels take priority over payload's {@link Event} annotation.
     *
     * @param <T>      the payload type
     * @param payload  the domain event to wrap
     * @param channels channel classes for routing
     * @return new envelope instance
     * @throws IllegalArgumentException if channels array is empty
     */
    @SafeVarargs
    public static <T> Envelope<T> of(T payload, Class<? extends EventChannel>... channels) {
        Objects.requireNonNull(channels, "channels must not be null");
        if (channels.length == 0) {
            throw new IllegalArgumentException("At least one channel must be specified");
        }
        return new Envelope<>(
                UUID.randomUUID(),
                null,
                Instant.now(),
                payload,
                Map.of(),
                List.of(channels)
        );
    }

    /**
     * @return the unique event identifier
     */
    @JsonGetter("eventId")
    @Override
    public UUID eventId() {
        return eventId;
    }

    /**
     * @return the process identifier for correlation (e.g., saga ID)
     */
    @JsonGetter("processId")
    @Override
    public UUID processId() {
        return processId;
    }

    /**
     * @return the timestamp when the event occurred
     */
    @JsonGetter("occurredAt")
    @Override
    public Instant occurredAt() {
        return occurredAt;
    }

    /**
     * @return the wrapped event payload
     */
    @JsonGetter("payload")
    public T payload() {
        return payload;
    }

    /**
     * @return additional metadata associated with the event
     */
    @JsonGetter("metadata")
    public Map<String, String> metadata() {
        return metadata;
    }

    /**
     * @return {@link Envelope} class as the event type
     */
    @Override
    public Class<? extends Event> type() {
        return Envelope.class;
    }

    /**
     * Resolves channels in the following priority:
     * <ol>
     *   <li>Channels specified via factory method</li>
     *   <li>Channels from payload's {@link Event} annotation</li>
     *   <li>{@link InternalEventChannel} as default</li>
     * </ol>
     *
     * @return resolved list of channel classes
     */
    @Override
    public List<Class<? extends EventChannel>> channels() {
        if (targetChannels != null) {
            return targetChannels;
        }
        var annotation = payload.getClass().getAnnotation(io.github.vovten.eventflow.event.annotation.Event.class);
        if (annotation != null) {
            return Arrays.asList(annotation.channels());
        }
        return List.of(InternalEventChannel.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Envelope<?> envelope = (Envelope<?>) o;
        return eventId.equals(envelope.eventId);
    }

    @Override
    public int hashCode() {
        return eventId.hashCode();
    }

    @Override
    public String toString() {
        String payloadJson;
        try {
            payloadJson = EventUtils.toJson(payload);
        } catch (Exception e) {
            payloadJson = payload.getClass().getSimpleName();
        }
        return String.format("Envelope{eventId=%s, processId=%s, occurredAt=%s, payload=%s}",
                eventId, processId, occurredAt, payloadJson);
    }
}