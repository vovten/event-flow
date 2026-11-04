package io.github.vovten.eventflow;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.retry.ResolvedRetryPolicy;

/**
 * Handler for events occurring in the application.
 * <p>
 * This is the base interface for all event handlers. It defines a single method
 * for processing events.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see EventSubscriber
 */
public interface EventHandler {

    /**
     * Method that processes the event
     *
     * @param event the event to process
     */
    void onEvent(Event event);

    /**
     * Returns the name of this handler for logging/tracing purposes.
     * Default implementation returns the simple class name.
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
     *
     * @return the resolved policy, or {@code null} when the default policy applies
     * @since 1.4.0
     */
    default ResolvedRetryPolicy resolvedRetryPolicy() {
        return null;
    }
}
