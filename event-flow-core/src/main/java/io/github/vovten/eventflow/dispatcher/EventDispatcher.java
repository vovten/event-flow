package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.Event;

import java.util.function.Consumer;

/**
 * <p>Event dispatcher.
 * <p>Receives an event from the bus and delivers it to listeners.
 *
 * @author Vladimir Aleshkov
 * @since 2024-11-20
 */
public interface EventDispatcher {

    /**
     * Redirect the event to appropriate listeners
     *
     * @param event the event
     */
    void dispatch(Event event);

    /**
     * Register a listener
     *
     * @param listener the listener
     */
    void register(Object listener);

    /**
     * Check if a listener is registered
     *
     * @param listener the listener
     * @return true if the listener is registered, false otherwise
     */
    boolean isRegistered(Object listener);

    /**
     * Start the dispatcher and all configured transports.
     * <p>
     * This method activates all transports and begins delivering events to
     * registered handlers. The provided {@code dispatchConsumer} is used to
     * dispatch events from transports, enabling decorator pattern support.
     *
     * @param dispatchConsumer the consumer to dispatch events to
     */
    void start(Consumer<Event> dispatchConsumer);

    /**
     * Stop the dispatcher and all configured transports.
     * <p>
     * This method gracefully shuts down all transports and releases resources.
     */
    void stop();
}
