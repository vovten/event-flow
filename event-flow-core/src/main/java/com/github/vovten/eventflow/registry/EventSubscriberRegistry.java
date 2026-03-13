package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventHandler;
import com.github.vovten.eventflow.EventSubscriber;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry that discovers and manages event subscribers implementing the {@link EventSubscriber} interface.
 * <p>
 * This registry provides a type-safe approach to event listening by requiring subscribers
 * to explicitly implement the {@code EventSubscriber} interface. This makes subscriber
 * capabilities clear from the class definition and enables IDE support for discovery.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Interface-based subscriber discovery — explicit contract</li>
 *   <li>Support for multiple event types per subscriber</li>
 *   <li>Support for generic Event.class subscribers</li>
 *   <li>Type-safe event handling</li>
 * </ul>
 * <p>
 * <b>Subscriber implementation example:</b>
 * <pre>{@code
 * public class OrderCreatedSubscriber implements EventSubscriber {
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
 * // Register subscriber
 * EventHandlerRegistry registry = new EventSubscriberRegistry();
 * registry.register(new OrderCreatedSubscriber());
 * }</pre>
 * <p>
 * <b>Multiple event types:</b>
 * A single subscriber can handle multiple event types:
 * <pre>{@code
 * public class MultiEventSubscriber implements EventSubscriber {
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
 * <b>Generic subscribers:</b>
 * Subscribers that return {@code Event.class} from {@code events()} will receive
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
 * @see EventSubscriber
 * @see Event
 * @see EventListenerRegistry
 */
public class EventSubscriberRegistry implements EventHandlerRegistry {

    /**
     * Map of event types to subscribers.
     * Key: Event class
     * Value: List of EventSubscriber instances
     */
    private final Map<Class<? extends Event>, List<EventSubscriber>> eventSubscribers;

    /**
     * Creates a new interface-based event subscriber registry.
     */
    public EventSubscriberRegistry() {
        this.eventSubscribers = new HashMap<>();
    }

    /**
     * Get all subscribers for the specified event type.
     * <p>
     * This method returns:
     * <ol>
     *   <li>Subscribers registered for the exact event class</li>
     *   <li>Subscribers registered for generic Event.class (if any)</li>
     * </ol>
     *
     * @param event the event to find subscribers for
     * @return list of subscribers for this event type
     */
    @Override
    public List<EventHandler> getHandlers(Event event) {
        List<EventHandler> handlers = new ArrayList<>(eventSubscribers.getOrDefault(event.getClass(), List.of()));

        // Also add subscribers for generic Event.class
        if (eventSubscribers.containsKey(Event.class)) {
            handlers.addAll(eventSubscribers.get(Event.class));
        }
        return handlers;
    }

    /**
     * Get the number of registered event types.
     * <p>
     * Note: This counts unique event types, not individual subscriber instances.
     *
     * @return number of registered event types
     */
    @Override
    public int handlerCount() {
        return eventSubscribers.size();
    }

    /**
     * Register an EventSubscriber instance.
     * <p>
     * The subscriber is registered for all event types returned by
     * {@link EventSubscriber#events()}.
     * <p>
     * Objects that do not implement EventSubscriber are silently ignored.
     *
     * @param eventSubscriber the subscriber to register
     */
    @Override
    public void register(Object eventSubscriber) {
        if (eventSubscriber instanceof EventSubscriber subscriber) {
            registerSubscriber(subscriber);
        }
    }

    /**
     * Unregister an EventSubscriber instance.
     *
     * @param eventSubscriber the subscriber to unregister
     * @return true if the subscriber was found and removed, false otherwise
     */
    @Override
    public boolean unregister(Object eventSubscriber) {
        if (!(eventSubscriber instanceof EventSubscriber)) {
            return false;
        }
        for (List<EventSubscriber> subscribers : eventSubscribers.values()) {
            if (subscribers.remove(eventSubscriber)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if an object is registered as an EventSubscriber.
     *
     * @param eventSubscriber the subscriber to check
     * @return true if registered, false otherwise
     */
    @Override
    public boolean isRegistered(Object eventSubscriber) {
        if (!(eventSubscriber instanceof EventSubscriber)) {
            return false;
        }
        return eventSubscribers.values().stream()
                .anyMatch(subscribers -> subscribers.contains(eventSubscriber));
    }

    /**
     * Merging is not supported for this registry implementation.
     *
     * @param registry the registry to merge
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public void merge(EventHandlerRegistry registry) {
        throw new UnsupportedOperationException("Merging registries is not supported");
    }

    /**
     * Register an EventSubscriber for all its declared event types.
     * <p>
     * The subscriber is added to the list for each event type it declares interest in.
     * Duplicate registrations are ignored.
     *
     * @param subscriber the subscriber to register
     */
    protected void registerSubscriber(EventSubscriber subscriber) {
        for (Class<? extends Event> event : subscriber.events()) {
            var subscribers = eventSubscribers.computeIfAbsent(event, k -> new ArrayList<>());
            if (!subscribers.contains(subscriber)) {
                subscribers.add(subscriber);
            }
        }
    }

    @Override
    public String name() {
        return "interface";
    }
}
