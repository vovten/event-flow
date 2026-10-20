package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.DefaultEventBuilder;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.EventBuilder;
import io.github.vovten.eventflow.transport.SendResults;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Core interface for publishing events.
 * <p>
 * Implementations handle event delivery to configured channels (internal queues, external brokers).
 * Publishing is always asynchronous to avoid blocking the caller.
 * <p>
 * <b>Two-publication-paths contract:</b>
 * <ul>
 *   <li>{@link #publish(Object)} — high-level API: wraps any payload in an {@link Envelope}
 *       (an already-built Envelope passes through unchanged)</li>
 *   <li>{@link #send(Event)} — low-level API: delivers a pre-built {@link Event} as-is
 *       (used by library internals such as retry re-delivery)</li>
 * </ul>
 * Keeping them under distinct names removes the overload-resolution trap where a
 * payload implementing {@link Event} was silently published raw, without an
 * {@link Envelope}, and its lifecycle status was never acknowledged.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public interface EventPublisher {

    /**
     * Deliver a pre-built event (typically an {@link Envelope}) to its channels as-is,
     * without any wrapping.
     * <p>
     * Low-level API: use when the event is already built and must reappear
     * unchanged downstream (e.g. re-publishing a deserialized stored event).
     * Most application code should prefer {@link #publish(Object)} or
     * {@link #prepare(Object)} instead.
     *
     * @param event the pre-built event to deliver
     * @return CompletableFuture that completes with SendResults
     */
    CompletableFuture<SendResults> send(Event event);

    /**
     * Publish any Object as an event wrapped in an {@link Envelope} with auto-generated metadata.
     * <p>
     * Allows publishing plain Java objects (POJO/record) directly without any annotations.
     * The payload is automatically wrapped in an Envelope with:
     * <ul>
     *   <li>eventId - randomly generated UUID</li>
     *   <li>processId - null (use {@link #prepare(Object)} or {@link #publish(UUID, Object)} to set)</li>
     *   <li>occurredAt - current timestamp</li>
     * </ul>
     * <p>
     * An already-built {@link Envelope} is delivered unchanged (guarded against
     * double wrapping); any other {@link Event}-implementing payload is wrapped
     * like a plain object, so all high-level publications share the Envelope
     * lifecycle path.
     *
     * @param <T>     the payload type (any Object)
     * @param payload the object to publish as an event
     * @return CompletableFuture that completes with SendResults
     */
    default <T> CompletableFuture<SendResults> publish(T payload) {
        Event event = payload instanceof Envelope<?> envelope ? envelope : Envelope.of(payload);
        return send(event);
    }

    /**
     * Publish any Object as an event with a specific processId.
     * <p>
     * Convenience method that wraps the payload in an {@link Envelope} with
     * the given processId, auto-generated eventId, and current timestamp.
     * Equivalent to {@code publish(Envelope.of(payload, processId))}.
     * <p>
     * An already-built {@link Envelope} is delivered with its own processId
     * (the argument is ignored for envelopes).
     * <p>
     * Example:
     * <pre>{@code
     * UUID sagaId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
     * publisher.publish(sagaId, new OrderCreated(orderId, email));
     * }</pre>
     *
     * @param <T>       the payload type (any Object)
     * @param processId the process identifier (e.g., saga ID) for correlation
     * @param payload   the object to publish as an event
     * @return CompletableFuture that completes with SendResults
     */
    default <T> CompletableFuture<SendResults> publish(UUID processId, T payload) {
        Event event = payload instanceof Envelope<?> envelope ? envelope : Envelope.of(payload, processId);
        return send(event);
    }

    /**
     * Create an event builder for fine-grained control over envelope metadata.
     * <p>
     * Example:
     * <pre>{@code
     * publisher.prepare(new OrderCreated(orderId, email))
     *     .withProcessId(processId)
     *     .withMetadata("source", "web-api")
     *     .publish();
     * }</pre>
     *
     * @param <T>     the payload type
     * @param payload the domain object to wrap
     * @return new EventBuilder instance
     */
    default <T> EventBuilder<T> prepare(T payload) {
        return new DefaultEventBuilder<>(this, payload);
    }
}
