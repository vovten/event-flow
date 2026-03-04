package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;

import java.util.List;

/**
 * Registry of event listeners
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public interface EventListenerRegistry {
    /**
     * Get listeners for the specified event type
     *
     * @param event the event
     * @return list of listeners that handle this event type
     */
    List<EventListener> getListeners(Event event);

    /**
     * Number of listeners in the registry
     *
     * @return number of listeners
     */
    int listenerCount();

    /**
     * Register a listener in the registry
     *
     * @param eventListener listener that implements the EventListener interface
     *                     or has methods annotated with @EventListener
     */
    void register(Object eventListener);

    /**
     * Unregister a listener from the registry
     *
     * @param eventListener the listener to unregister
     * @return true if the listener was successfully unregistered, false otherwise
     */
    boolean unregister(Object eventListener);

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
