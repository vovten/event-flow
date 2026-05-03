package io.github.vovten.eventflow.event;

import io.github.vovten.eventflow.transport.SendResults;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Builder for constructing {@link Envelope} instances with optional technical metadata.
 * <p>
 * Allows fluent configuration of eventId, traceId, occurredAt, and custom metadata
 * before publishing a domain event.
 *
 * @param <T> the type of the payload being built
 * @author Vladimir Aleshkov
 * @since 2026-05-03
 */
public interface EventBuilder<T> {

    /**
     * Set custom event ID.
     *
     * @param eventId the unique event identifier
     * @return this builder
     */
    EventBuilder<T> withEventId(UUID eventId);

    /**
     * Set custom event ID from string.
     *
     * @param eventId the unique event identifier as string
     * @return this builder
     */
    EventBuilder<T> withEventId(String eventId);

    /**
     * Set trace ID for correlation.
     *
     * @param traceId the trace identifier
     * @return this builder
     */
    EventBuilder<T> withTraceId(String traceId);

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
    EventBuilder<T> withMetadata(java.util.Map<String, String> metadata);

    /**
     * Build the envelope and publish via the associated publisher.
     *
     * @return CompletableFuture with send results
     */
    CompletableFuture<SendResults> publish();
}