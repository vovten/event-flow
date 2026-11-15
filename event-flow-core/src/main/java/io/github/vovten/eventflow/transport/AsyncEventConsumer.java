package io.github.vovten.eventflow.transport;

import io.github.vovten.eventflow.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Consumer of incoming events that reports processing completion.
 * <p>
 * Unlike {@link java.util.function.Consumer}, the returned future completes when
 * the consumer has finished processing the event (successfully or exceptionally).
 * Transports can use the completion signal to acknowledge the source only after
 * processing has finished — the key ingredient of at-least-once delivery.
 * <p>
 * {@link io.github.vovten.eventflow.dispatcher.EventDispatcher#dispatch(Event)}
 * naturally implements this interface, so transports are typically started with
 * {@code dispatcher.start(dispatcher::dispatch)}.
 * <p>
 * Any exception — thrown synchronously or delivered through an exceptional
 * future — is treated as a processing failure. Transports apply their own
 * failure policy: {@link io.github.vovten.eventflow.transport.incoming.KafkaInTransport}
 * blocks the partition until a rebalance, a restart or an explicit unblock,
 * preserving per-partition ordering.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@FunctionalInterface
public interface AsyncEventConsumer {

    /**
     * Accept the event for processing.
     * <p>
     * The returned future must complete when processing of the event has
     * finished. Exceptional completion signals a processing failure; a future
     * that never completes keeps the event unacknowledged.
     *
     * @param event the event to process
     * @return future completing with the processing outcome (never null)
     */
    CompletableFuture<?> accept(Event event);
}
