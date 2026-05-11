package io.github.vovten.eventflow;

import io.github.vovten.eventflow.event.Event;

/**
 * Handler for events occurring in the application.
 * <p>
 * This is the base interface for all event handlers. It defines a single method
 * for processing events.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
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
}
