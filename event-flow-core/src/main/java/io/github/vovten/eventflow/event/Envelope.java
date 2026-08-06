package io.github.vovten.eventflow.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.lifecycle.EventLifecycle;
import io.github.vovten.eventflow.lifecycle.LifecycleResolver;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
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
 * @since 1.1.0
 */
public final class Envelope<T> implements TraceableEvent {

    private final UUID eventId;
    private final UUID processId;
    private final Instant occurredAt;
    private final Map<String, String> metadata;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final T payload;

    @JsonIgnore
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
        this.metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
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
        this.metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
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
     * Create envelope with specified eventId, null processId, and current timestamp.
     * Channels are resolved from payload's {@link Event} annotation, or default to internal.
     *
     * @param <T>     the payload type
     * @param eventId the explicit event identifier
     * @param payload the event to wrap
     * @return new envelope instance
     */
    public static <T> Envelope<T> of(UUID eventId, T payload) {
        return new Envelope<>(
                eventId,
                null,
                Instant.now(),
                payload,
                Map.of(),
                null
        );
    }

    /**
     * Create envelope with specified eventId, specified processId, and current timestamp.
     * Channels are resolved from payload's {@link Event} annotation, or default to internal.
     *
     * @param <T>        the payload type
     * @param eventId    the explicit event identifier
     * @param processId  the process identifier (e.g., saga ID)
     * @param payload    the domain event to wrap
     * @return new envelope instance
     */
    public static <T> Envelope<T> of(UUID eventId, UUID processId, T payload) {
        return new Envelope<>(
                eventId,
                processId,
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
     * Returns a new {@code Envelope} with the same properties plus an additional
     * metadata entry. The explicit {@code targetChannels} (if any) are preserved.
     *
     * @param key   metadata key
     * @param value metadata value
     * @return a new envelope with the merged metadata
     */
    public Envelope<T> withAdditionalMetadata(String key, String value) {
        Map<String, String> newMetadata = new HashMap<>(this.metadata);
        newMetadata.put(key, value);
        return new Envelope<>(
                this.eventId, this.processId, this.occurredAt, this.payload,
                newMetadata, this.targetChannels
        );
    }

    /**
     * @return the actual type of the wrapped payload
     */
    @Override
    public Class<?> type() {
        return payload.getClass();
    }

    /**
     * Resolves channels in the following priority:
     * <ol>
     *   <li>Channels specified via factory method</li>
     *   <li>Channels from payload's {@link Event} annotation</li>
     *   <li>Channels from payload's {@link Event} interface override</li>
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
        if (payload instanceof Event evt) {
            return evt.channels();
        }
        return List.of(InternalEventChannel.class);
    }

    /**
     * Resolves the lifecycle level for this envelope.
     * <p>
     * Resolution priority:
     * <ol>
     *   <li>{@link Event @Event} annotation on the payload class</li>
     *   <li>{@link Event#lifecycle()} default method (if payload implements Event)</li>
     *   <li>{@link EventLifecycle#PERSISTED} as fallback for POJO payloads</li>
     * </ol>
     *
     * @return resolved lifecycle level
     */
    @Override
    public EventLifecycle lifecycle() {
        return LifecycleResolver.standard().resolve(this);
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
        return String.format("Envelope{eventId=%s, processId=%s, occurredAt=%s, payload=%s}",
                eventId, processId, occurredAt, payload);
    }
}