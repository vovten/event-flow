package io.github.vovten.eventflow;

import java.util.List;

/**
 * Subscriber for events occurring in the application.
 * <p>
 * A subscriber can handle multiple event types. It extends {@link EventHandler}
 * and adds the ability to declare which event types it handles.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public interface EventSubscriber extends EventHandler {

    /**
     * @return list of event types handled by this subscriber
     */
    List<Class<?>> events();

    /**
     * Named processing-retry policy reference for this subscriber.
     * <p>
     * The named policy must be declared with {@code @RetryPolicy(name = "...")}.
     * An empty value means the configured default policy. An unknown name fails
     * at startup.
     *
     * @return the named policy reference, or an empty string for the default policy
     * @since 1.4.0
     */
    default String retryPolicy() {
        return "";
    }
}
