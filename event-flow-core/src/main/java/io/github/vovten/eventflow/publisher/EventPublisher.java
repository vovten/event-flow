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
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public interface EventPublisher {

    /**
     * Publish the event asynchronously.
     *
     * @param event the event to publish
     * @return CompletableFuture that completes with SendResults
     */
    CompletableFuture<SendResults> publish(Event event);

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
     *
     * @param <T>     the payload type (any Object)
     * @param payload the object to publish as an event
     * @return CompletableFuture that completes with SendResults
     */
    default <T> CompletableFuture<SendResults> publish(T payload) {
        return publish(Envelope.of(payload));
    }

    /**
     * Publish any Object as an event with a specific processId.
     * <p>
     * Convenience method that wraps the payload in an {@link Envelope} with
     * the given processId, auto-generated eventId, and current timestamp.
     * Equivalent to {@code publish(Envelope.of(payload, processId))}.
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
        return publish(Envelope.of(payload, processId));
    }

    /**
     * Create an event builder for fine-grained control over envelope metadata.
     * <p>
     * Example:
     * <pre>{@code
     * publisher.prepare(new OrderCreated(orderId, email))
     *     .withTraceId("trace-123")
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