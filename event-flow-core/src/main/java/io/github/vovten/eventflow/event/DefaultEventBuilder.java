package io.github.vovten.eventflow.event;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.transport.SendResults;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link EventBuilder}.
 *
 * @param <T> the payload type
 * @since 1.1.0
 */
public final class DefaultEventBuilder<T> implements EventBuilder<T> {

    private final EventPublisher publisher;
    private final T payload;
    private UUID processId;
    private Instant occurredAt;
    private final Map<String, String> metadata;
    private List<Class<? extends EventChannel>> channels;

    public DefaultEventBuilder(EventPublisher publisher, T payload) {
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.occurredAt = Instant.now();
        this.metadata = new HashMap<>();
    }

    @Override
    public EventBuilder<T> withProcessId(UUID processId) {
        this.processId = processId;
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
    public EventBuilder<T> withChannels(List<Class<? extends EventChannel>> channels) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("At least one channel must be specified");
        }
        this.channels = channels;
        return this;
    }

    @Override
    public CompletableFuture<SendResults> publish() {
        Envelope<T> envelope = new Envelope<>(
                UUID.randomUUID(),
                processId,
                occurredAt,
                payload,
                Map.copyOf(metadata),
                channels
        );
        return publisher.publish(envelope);
    }
}