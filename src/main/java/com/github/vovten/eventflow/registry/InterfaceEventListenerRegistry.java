package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry that discovers and manages event listeners implementing the {@link EventListener} interface.
 * <p>
 * This registry provides a type-safe approach to event listening by requiring listeners
 * to explicitly implement the {@code EventListener} interface. This makes listener
 * capabilities clear from the class definition and enables IDE support for discovery.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Interface-based listener discovery — explicit contract</li>
 *   <li>Support for multiple event types per listener</li>
 *   <li>Support for generic Event.class listeners</li>
 *   <li>Type-safe event handling</li>
 * </ul>
 * <p>
 * <b>Listener implementation example:</b>
 * <pre>{@code
 * public class OrderCreatedListener implements EventListener {
 *
 *     @Override
 *     public List<Class<? extends Event>> events() {
 *         return List.of(OrderCreatedEvent.class);
 *     }
 *
 *     @Override
 *     public void onEvent(Event event) {
 *         OrderCreatedEvent orderEvent = (OrderCreatedEvent) event;
 *         System.out.println("Order created: " + orderEvent.getOrderId());
 *     }
 * }
 *
 * // Register listener
 * EventListenerRegistry registry = new InterfaceEventListenerRegistry();
 * registry.register(new OrderCreatedListener());
 * }</pre>
 * <p>
 * <b>Multiple event types:</b>
 * A single listener can handle multiple event types:
 * <pre>{@code
 * public class MultiEventListener implements EventListener {
 *
 *     @Override
 *     public List<Class<? extends Event>> events() {
 *         return List.of(
 *             OrderCreatedEvent.class,
 *             OrderUpdatedEvent.class,
 *             OrderCancelledEvent.class
 *         );
 *     }
 *
 *     @Override
 *     public void onEvent(Event event) {
 *         // Handle all order events
 *     }
 * }
 * }</pre>
 * <p>
 * <b>Generic listeners:</b>
 * Listeners that return {@code Event.class} from {@code events()} will receive
 * all events, regardless of their specific type.
 * <p>
 * <b>Comparison with annotation-based registry:</b>
 * <ul>
 *   <li><b>Interface-based:</b> Explicit contract, IDE support, type-safe</li>
 *   <li><b>Annotation-based:</b> Less boilerplate, flexible method naming</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-07
 * @see EventListener
 * @see Event
 * @see AnnotationEventListenerRegistry
 */
public class InterfaceEventListenerRegistry implements EventListenerRegistry {

    /**
     * Map of event types to listeners.
     * Key: Event class
     * Value: List of EventListener instances
     */
    private final Map<Class<? extends Event>, List<EventListener>> eventListeners;

    /**
     * Creates a new interface-based event listener registry.
     */
    public InterfaceEventListenerRegistry() {
        this.eventListeners = new HashMap<>();
    }

    /**
     * Get all listeners for the specified event type.
     * <p>
     * This method returns:
     * <ol>
     *   <li>Listeners registered for the exact event class</li>
     *   <li>Listeners registered for generic Event.class (if any)</li>
     * </ol>
     *
     * @param event the event to find listeners for
     * @return list of listeners for this event type
     */
    @Override
    public List<EventListener> getListeners(Event event) {
        List<EventListener> listeners = new ArrayList<>(eventListeners.getOrDefault(event.getClass(), List.of()));

        // Also add listeners for generic Event.class
        if (eventListeners.containsKey(Event.class)) {
            listeners.addAll(eventListeners.get(Event.class));
        }
        return listeners;
    }

    /**
     * Get the number of registered event types.
     * <p>
     * Note: This counts unique event types, not individual listener instances.
     *
     * @return number of registered event types
     */
    @Override
    public int listenerCount() {
        return eventListeners.size();
    }

    /**
     * Register an EventListener instance.
     * <p>
     * The listener is registered for all event types returned by
     * {@link EventListener#events()}.
     * <p>
     * Objects that do not implement EventListener are silently ignored.
     *
     * @param eventListener the listener to register
     */
    @Override
    public void register(Object eventListener) {
        if (eventListener instanceof EventListener listener) {
            registerListener(listener);
        }
    }

    /**
     * Unregister an EventListener instance.
     *
     * @param eventListener the listener to unregister
     * @return true if the listener was found and removed, false otherwise
     */
    @Override
    public boolean unregister(Object eventListener) {
        if (!(eventListener instanceof EventListener)) {
            return false;
        }
        for (List<EventListener> listeners : eventListeners.values()) {
            if (listeners.remove(eventListener)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if an object is registered as an EventListener.
     *
     * @param eventListener the listener to check
     * @return true if registered, false otherwise
     */
    @Override
    public boolean isRegistered(Object eventListener) {
        if (!(eventListener instanceof EventListener)) {
            return false;
        }
        return eventListeners.values().stream()
                .anyMatch(listeners -> listeners.contains(eventListener));
    }

    /**
     * Merging is not supported for this registry implementation.
     *
     * @param registry the registry to merge
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public void merge(EventListenerRegistry registry) {
        throw new UnsupportedOperationException("Merging registries is not supported");
    }

    /**
     * Register an EventListener for all its declared event types.
     * <p>
     * The listener is added to the list for each event type it declares interest in.
     * Duplicate registrations are ignored.
     *
     * @param listener the listener to register
     */
    protected void registerListener(EventListener listener) {
        for (Class<? extends Event> event : listener.events()) {
            var listeners = eventListeners.computeIfAbsent(event, k -> new ArrayList<>());
            if (!listeners.contains(listener)) {
                listeners.add(listener);
            }
        }
    }
}
