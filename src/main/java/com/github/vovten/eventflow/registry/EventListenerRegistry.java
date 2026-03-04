package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;

/**
 * Registry of event listeners
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public interface EventListenerRegistry {
    /**
     * Dispatch the event to listeners
     *
     * @param event the event
     * @return true if the event was dispatched to at least one listener, false otherwise
     */
    boolean dispatch(Event event);

    /**
     * Number of listeners in the registry
     *
     * @return number of listeners
     */
    int listenerCount();

    /**
     * Check if the registry has any listeners
     *
     * @return true if the registry has listeners, false otherwise
     */
    boolean hasListeners();

    /**
     * Register a listener in the registry
     *
     * @param eventListener listener that either implements the EventListener interface
     *                     or has methods annotated with @EventListener
     */
    void register(Object eventListener);

    /**
     * Check if a listener is registered in the registry
     *
     * @param eventListener the listener
     * @return true if the listener is registered in the registry, false otherwise
     */
    boolean isRegistered(Object eventListener);

    /**
     * Merge another listener registry into this registry
     *
     * @param registry the registry to merge
     */
    void merge(EventListenerRegistry registry);
}
