package io.github.vovten.eventflow;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.retry.ResolvedRetryPolicy;

/**
 * Handler for events occurring in the application.
 * <p>
 * This is the base interface for all event handlers. A handler is invoked by
 * the dispatcher with the matching {@link Event} instance. Handlers can be
 * registered through annotated listener methods ({@link EventListener}),
 * through {@link EventSubscriber}, or directly in an
 * {@link io.github.vovten.eventflow.registry.EventHandlerRegistry}.
 * <p>
 * <b>Retries and idempotency.</b> A handler may be invoked more than once for
 * the same logical event: local retries re-run the failing handler in-process,
 * and durable processing retries re-publish the event and re-run all handlers.
 * Implementations must therefore be idempotent and safe for repeated
 * invocation.
 * <p>
 * <b>Failures.</b> An exception thrown from {@link #onEvent(Event)} is treated
 * as a handler failure. The dispatcher classifies it with the resolved retry
 * policy and either schedules another attempt or reports a failure
 * acknowledgment.
 * <p>
 * <b>Concurrency.</b> Handlers may be invoked concurrently for different
 * events. Implementations that keep mutable state must provide their own
 * synchronization.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see EventSubscriber
 * @see EventListener
 * @see ResolvedRetryPolicy
 */
public interface EventHandler {

    /**
     * Processes the event.
     * <p>
     * The method may be invoked multiple times for the same logical event
     * because of retries; implementations must be idempotent.
     *
     * @param event the event to process
     * @throws RuntimeException if processing fails; the dispatcher classifies
     *                          the failure using the resolved retry policy
     */
    void onEvent(Event event);

    /**
     * Returns the name of this handler for logging and diagnostics.
     * <p>
     * The default implementation returns the simple class name. Method-based
     * handlers override it with the listener method identifier.
     *
     * @return the handler name
     */
    default String name() {
        return getClass().getSimpleName();
    }

    /**
     * Returns the resolved processing-retry policy for this handler.
     * <p>
     * The registry resolves named policies at registration time. A {@code null}
     * value means the handler has no explicitly resolved policy and the
     * dispatcher's default policy applies.
     * <p>
     * Named policies are declared with {@link io.github.vovten.eventflow.retry.RetryPolicy}
     * and referenced with {@code @EventListener(retryPolicy = "...")} or
     * {@link EventSubscriber#retryPolicy()}.
     *
     * @return the resolved policy, or {@code null} when the default policy applies
     * @since 1.4.0
     */
    default ResolvedRetryPolicy resolvedRetryPolicy() {
        return null;
    }
}
