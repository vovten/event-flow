package io.github.vovten.eventflow.event;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.transport.SendResults;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Builder for constructing {@link Envelope} instances with optional technical metadata.
 * <p>
 * Allows fluent configuration of eventId, processId, occurredAt, custom metadata,
 * and target channels before publishing a domain event.
 *
 * @param <T> the type of the payload being built
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public interface EventBuilder<T> {

    /**
     * Set process ID for correlation (e.g., saga ID).
     *
     * @param processId the process identifier
     * @return this builder
     */
    EventBuilder<T> withProcessId(UUID processId);

    /**
     * Set custom occurrence timestamp.
     *
     * @param occurredAt the event timestamp
     * @return this builder
     */
    EventBuilder<T> withOccurredAt(Instant occurredAt);

    /**
     * Add single metadata entry.
     *
     * @param key   the metadata key
     * @param value the metadata value
     * @return this builder
     */
    EventBuilder<T> withMetadata(String key, String value);

    /**
     * Add multiple metadata entries.
     *
     * @param metadata the metadata map to merge
     * @return this builder
     */
    EventBuilder<T> withMetadata(Map<String, String> metadata);

    /**
     * Set target event channels for routing.
     * <p>
     * Channels specified here take priority over any {@link io.github.vovten.eventflow.event.annotation.Event} annotation
     * on the payload class. If channels are set via this method, the
     * annotation's channel configuration is ignored.
     *
     * @param c1 first channel class
     * @return this builder
     * @throws UnsupportedOperationException if implementation does not support channel specification
     */
    default EventBuilder<T> withChannels(Class<? extends EventChannel> c1) {
        return withChannels(List.of(c1));
    }

    /**
     * Set target event channels for routing.
     * <p>
     * Channels specified here take priority over any {@link io.github.vovten.eventflow.event.annotation.Event} annotation
     * on the payload class. If channels are set via this method, the
     * annotation's channel configuration is ignored.
     *
     * @param c1 first channel class
     * @param c2 second channel class
     * @return this builder
     * @throws UnsupportedOperationException if implementation does not support channel specification
     */
    default EventBuilder<T> withChannels(Class<? extends EventChannel> c1, Class<? extends EventChannel> c2) {
        return withChannels(List.of(c1, c2));
    }

    /**
     * Set target event channels for routing.
     * <p>
     * Channels specified here take priority over any {@link io.github.vovten.eventflow.event.annotation.Event} annotation
     * on the payload class. If channels are set via this method, the
     * annotation's channel configuration is ignored.
     *
     * @param c1 first channel class
     * @param c2 second channel class
     * @param c3 third channel class
     * @return this builder
     * @throws UnsupportedOperationException if implementation does not support channel specification
     */
    default EventBuilder<T> withChannels(Class<? extends EventChannel> c1,
                                         Class<? extends EventChannel> c2,
                                         Class<? extends EventChannel> c3) {
        return withChannels(List.of(c1, c2, c3));
    }

    /**
     * Set target event channels for routing.
     * <p>
     * Channels specified here take priority over any {@link io.github.vovten.eventflow.event.annotation.Event} annotation
     * on the payload class. If channels are set via this method, the
     * annotation's channel configuration is ignored.
     *
     * @param channels target channel classes for event routing
     * @return this builder
     * @throws UnsupportedOperationException if implementation does not support channel specification
     */
    default EventBuilder<T> withChannels(List<Class<? extends EventChannel>> channels) {
        throw new UnsupportedOperationException("withChannels not implemented in " + getClass().getSimpleName());
    }

    /**
     * Set single target event channel for routing.
     * <p>
     * Shortcut for convenience.
     *
     * @param channel the target channel class
     * @return this builder
     */
    default EventBuilder<T> withChannel(Class<? extends EventChannel> channel) {
        return withChannels(List.of(channel));
    }

    /**
     * Build the envelope and publish via the associated publisher.
     *
     * @return CompletableFuture with send results
     */
    CompletableFuture<SendResults> publish();
}