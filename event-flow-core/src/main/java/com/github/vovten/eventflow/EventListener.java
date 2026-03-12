package com.github.vovten.eventflow;

import java.util.List;

/**
 * Listener for events occurring in the application
 *
 * @author Vladimir Aleshkov
 * @since 2024-11-21
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
