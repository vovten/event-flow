package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.DefaultEventBuilder;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.EventBuilder;
import io.github.vovten.eventflow.transport.SendResults;

import java.util.concurrent.CompletableFuture;

/**
 * Event publisher interface.
 * <p>
 * Provides methods for publishing events:
 * <ul>
 *   <li>{@link #publish(Event)} - publish an event implementing {@link Event} interface</li>
 *   <li>{@link #publish(Object)} - publish any domain object wrapped in an {@link Envelope}</li>
 *   <li>{@link #prepare(Object)} - create a builder for fine-grained control over envelope metadata</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 2024-11-20
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
     * Publish any domain object wrapped in an {@link Envelope} with auto-generated metadata.
     * <p>
     * The payload is automatically wrapped in an Envelope with:
     * <ul>
     *   <li>eventId - randomly generated UUID</li>
     *   <li>traceId - null (use {@link #prepare(Object)} to set)</li>
     *   <li>occurredAt - current timestamp</li>
     *   <li>metadata - with payloadType key</li>
     * </ul>
     *
     * @param <T>     the payload type
     * @param payload the domain object to publish
     * @return CompletableFuture that completes with SendResults
     */
    default <T> CompletableFuture<SendResults> publish(T payload) {
        return publish(Envelope.of(payload));
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