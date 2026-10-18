package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;

import java.util.Objects;

/**
 * Strategy for resolving the Kafka record key from an event.
 * <p>
 * The key determines which Kafka partition receives the event: Kafka consistently
 * hashes the key, so events with the same key always land in the same partition
 * (as long as the topic partition count does not change). A {@code null} key makes
 * Kafka fall back to round-robin distribution.
 * <p>
 * <b>Built-in strategies:</b>
 * <ul>
 *   <li>{@link #none()} — no key, Kafka round-robin (default, backward compatible)</li>
 *   <li>{@link #processId()} — key is the event's {@code processId}, so all events
 *       of the same process/saga land in the same partition</li>
 *   <li>{@link #eventType()} — key is the event type class name, so all events of
 *       the same type land in the same partition (topic segmentation by type)</li>
 *   <li>{@link #metadata(String)} — key is a value from the {@link Envelope} metadata,
 *       set by the publisher at publish time</li>
 * </ul>
 * <p>
 * Custom strategies can be provided by implementing this functional interface
 * (e.g., a Spring {@code @Bean} referenced via {@code bean:&lt;name&gt;} in config).
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@FunctionalInterface
public interface EventKeyResolver {

    /**
     * Resolve the Kafka record key for the event.
     *
     * @param event the event to send
     * @return the Kafka record key, or {@code null} to let Kafka choose (round-robin)
     */
    String resolveKey(Event event);

    /**
     * No key strategy: always returns {@code null}, so Kafka distributes events
     * round-robin. This preserves the legacy behavior of {@code KafkaOutTransport}.
     *
     * @return a resolver that always returns {@code null}
     */
    static EventKeyResolver none() {
        return event -> null;
    }

    /**
     * Process-id strategy: uses the event's {@link TraceableEvent#processId()} as key,
     * so all events of the same process/saga land in the same partition preserving
     * per-process ordering.
     * <p>
     * Returns {@code null} (round-robin) when the event is not a {@link TraceableEvent}
     * or has no {@code processId}.
     *
     * @return a process-id based resolver
     */
    static EventKeyResolver processId() {
        return event -> (event instanceof TraceableEvent traceable
                && traceable.processId() != null)
                ? traceable.processId().toString()
                : null;
    }

    /**
     * Event-type strategy: uses the event type class name as key, so all events of
     * the same type land in the same partition. Useful for topic segmentation by
     * type, but may create hotspots when one event type dominates the volume.
     *
     * @return an event-type based resolver
     */
    static EventKeyResolver eventType() {
        return event -> event.type().getName();
    }

    /**
     * Metadata strategy: uses the value of the named entry from the
     * {@link Envelope} metadata as key. The publisher sets the value at publish
     * time, e.g. {@code publisher.prepare(payload).withMetadata("orderId", id)}.
     * <p>
     * Returns {@code null} (round-robin) when the event is not an {@link Envelope}
     * or the metadata entry is absent.
     *
     * @param metadataKey the metadata entry name to use as key
     * @return a metadata-based resolver
     * @throws NullPointerException if {@code metadataKey} is null
     */
    static EventKeyResolver metadata(String metadataKey) {
        Objects.requireNonNull(metadataKey, "metadataKey must not be null");
        return event -> (event instanceof Envelope<?> envelope)
                ? envelope.metadata().get(metadataKey)
                : null;
    }
}