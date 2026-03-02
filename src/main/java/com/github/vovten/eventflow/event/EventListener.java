package com.github.vovten.eventflow.event;

import java.util.List;

/**
 * Listener for events occurring in the application
 *
 * @author Vladimir Aleshkov, 21.11.2024.
 */
public interface EventListener {

    /**
     * @return list of event types handled by this listener
     */
    List<Class<? extends Event>> events();

    /**
     * Method that processes the event
     *
     * @param event the event to process
     */
    void onEvent(Event event);
}
