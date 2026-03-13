package com.github.vovten.eventflow;

import java.util.List;

/**
 * Subscriber for events occurring in the application.
 * <p>
 * A subscriber can handle multiple event types. It extends {@link EventHandler}
 * and adds the ability to declare which event types it handles.
 *
 * @author Vladimir Aleshkov
 * @since 2024-11-21
 */
public interface EventSubscriber extends EventHandler {

    /**
     * @return list of event types handled by this subscriber
     */
    List<Class<? extends Event>> events();
}
