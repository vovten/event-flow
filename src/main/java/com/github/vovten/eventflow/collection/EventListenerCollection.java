package com.github.vovten.eventflow.collection;

import com.github.vovten.eventflow.Event;

/**
 * Collection of event listeners
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public interface EventListenerCollection {
    /**
     * Pass the event to listeners
     *
     * @param event the event
     * @return true if the event was passed to at least one listener, false otherwise
     */
    boolean pass(Event event);

    /**
     * Size of the listener collection
     *
     * @return number of listeners
     */
    int size();

    /**
     * Check if the collection has any listeners
     *
     * @return true if the collection is empty, false otherwise
     */
    boolean isEmpty();

    /**
     * Add a listener to the collection
     *
     * @param eventListener listener that either implements the EventListener interface
     *                     or has methods annotated with @EventListener
     */
    void add(Object eventListener);

    /**
     * Check if a listener exists in the collection
     *
     * @param eventListener the listener
     * @return true if the listener is present in the collection, false otherwise
     */
    boolean contains(Object eventListener);

    /**
     * Add a listener collection to this collection
     *
     * @param eventListenerCollection the listener collection
     */
    void add(EventListenerCollection eventListenerCollection);
}
