package io.github.vovten.eventflow.event;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.transport.SendResults;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link EventBuilder}.
 *
 * @param <T> the payload type
 */
public final class DefaultEventBuilder<T> implements EventBuilder<T> {

    private static final String CHANNELS_KEY = "channels";

    private final EventPublisher publisher;
    private final T payload;
    private UUID eventId;
    private UUID processId;
    private Instant occurredAt;
    private final Map<String, String> metadata;
    private Class<? extends EventChannel>[] channels;

    public DefaultEventBuilder(EventPublisher publisher, T payload) {
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.eventId = UUID.randomUUID();
        this.occurredAt = Instant.now();
        this.metadata = new HashMap<>();
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
    @SafeVarargs
    public final EventBuilder<T> withChannels(Class<? extends EventChannel>... channels) {
        if (channels == null || channels.length == 0) {
            throw new IllegalArgumentException("At least one channel must be specified");
        }
        this.channels = channels;
        return this;
    }

    @Override
    public CompletableFuture<SendResults> publish() {
        Map<String, String> meta = new HashMap<>(metadata);
        if (channels != null) {
            meta.put(CHANNELS_KEY, Arrays.stream(channels)
                    .map(Class::getName)
                    .reduce((a, b) -> a + "," + b)
                    .orElse(""));
        }
        Envelope<T> envelope = new Envelope<>(
                eventId,
                processId,
                occurredAt,
                payload,
                Map.copyOf(meta)
        );
        return publisher.publish(envelope);
    }
}