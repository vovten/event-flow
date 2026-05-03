package io.github.vovten.eventflow.event;

import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.transport.SendResults;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link EventBuilder}.
 *
 * @param <T> the payload type
 */
public final class DefaultEventBuilder<T> implements EventBuilder<T> {

    private static final String PAYLOAD_TYPE_KEY = "payloadType";

    private final EventPublisher publisher;
    private final T payload;
    private UUID eventId;
    private String traceId;
    private Instant occurredAt;
    private final Map<String, String> metadata;

    public DefaultEventBuilder(EventPublisher publisher, T payload) {
        this.publisher = publisher;
        this.payload = payload;
        this.eventId = UUID.randomUUID();
        this.occurredAt = Instant.now();
        this.metadata = new HashMap<>();
        this.metadata.put(PAYLOAD_TYPE_KEY, payload.getClass().getName());
    }

    @Override
    public EventBuilder<T> withEventId(UUID eventId) {
        this.eventId = eventId;
        return this;
    }

    @Override
    public EventBuilder<T> withEventId(String eventId) {
        this.eventId = UUID.fromString(eventId);
        return this;
    }

    @Override
    public EventBuilder<T> withTraceId(String traceId) {
        this.traceId = traceId;
        return this;
    }

    @Override
    public EventBuilder<T> withOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
        return this;
    }

    @Override
    public EventBuilder<T> withMetadata(String key, String value) {
        this.metadata.put(key, value);
        return this;
    }

    @Override
    public EventBuilder<T> withMetadata(Map<String, String> metadata) {
        this.metadata.putAll(metadata);
        return this;
    }

    @Override
    public CompletableFuture<SendResults> publish() {
        Envelope<T> envelope = new Envelope<>(
                eventId,
                traceId,
                occurredAt,
                payload,
                Map.copyOf(metadata)
        );
        return publisher.publish(envelope);
    }
}