package io.github.vovten.eventflow.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Envelope wrapper for domain events that adds technical metadata.
 * <p>
 * Automatically captures: eventId (UUID), traceId (String), occurredAt (Instant).
 * Additional metadata can be added via {@link #metadata()}.
 * <p>
 * The envelope implements {@link Event} interface, so it passes through
 * existing transport infrastructure without modifications.
 *
 * @param <T> the type of the wrapped payload
 * @author Vladimir Aleshkov
 * @since 2026-05-03
 */
public final class Envelope<T> implements Event {

    private static final String PAYLOAD_TYPE_KEY = "payloadType";

    private final UUID eventId;
    private final String traceId;
    private final Instant occurredAt;
    private final T payload;
    private final Map<String, String> metadata;

    @JsonCreator
    public Envelope(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("traceId") String traceId,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("payload") T payload,
            @JsonProperty("metadata") Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.traceId = traceId;
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.metadata = Map.copyOf(metadata);
    }

    /**
     * Create envelope with auto-generated eventId, null traceId, and current timestamp.
     *
     * @param <T>     the payload type
     * @param payload the domain event to wrap
     * @return new envelope instance
     */
    public static <T> Envelope<T> of(T payload) {
        return new Envelope<>(
                UUID.randomUUID(),
                null,
                Instant.now(),
                payload,
                Map.of(PAYLOAD_TYPE_KEY, payload.getClass().getName())
        );
    }

    /**
     * Create envelope with auto-generated eventId, specified traceId, and current timestamp.
     *
     * @param <T>     the payload type
     * @param payload the domain event to wrap
     * @param traceId the trace ID
     * @return new envelope instance
     */
    public static <T> Envelope<T> of(T payload, String traceId) {
        return new Envelope<>(
                UUID.randomUUID(),
                traceId,
                Instant.now(),
                payload,
                Map.of(PAYLOAD_TYPE_KEY, payload.getClass().getName())
        );
    }

    /**
     * @return the unique event identifier
     */
    @JsonGetter("eventId")
    public UUID eventId() {
        return eventId;
    }

    /**
     * @return the trace ID for correlation
     */
    @JsonGetter("traceId")
    public String traceId() {
        return traceId;
    }

    /**
     * @return the timestamp when the event occurred
     */
    @JsonGetter("occurredAt")
    public Instant occurredAt() {
        return occurredAt;
    }

    /**
     * @return the wrapped domain event payload
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
     * @return default channels with {@link InternalEventChannel}
     */
    @Override
    public List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class);
    }

    /**
     * @return fully qualified class name of the wrapped payload
     */
    public String getPayloadType() {
        return metadata.get(PAYLOAD_TYPE_KEY);
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
        return String.format("Envelope{eventId=%s, traceId=%s, occurredAt=%s, payload=%s}",
                eventId, traceId, occurredAt, payload.getClass().getSimpleName());
    }
}