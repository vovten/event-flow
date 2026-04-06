package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.EventSubscriber;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.EventHandler;

import java.util.List;

/**
 * Registry for managing event handlers.
 * <p>
 * The registry is responsible for storing, discovering, and retrieving event handlers
 * that should be notified when specific events are published. It provides a centralized
 * mechanism for handler management within the event dispatching system.
 * <p>
 * <b>Key responsibilities:</b>
 * <ul>
 *   <li>Discover and register event handlers from various sources</li>
 *   <li>Match handlers to events based on event type</li>
 *   <li>Support dynamic registration and unregistration of handlers</li>
 *   <li>Enable composition of multiple registry implementations</li>
 * </ul>
 * <p>
 * <b>Handler discovery strategies:</b>
 * <ul>
 *   <li>{@link EventListenerRegistry} — discovers methods annotated with {@code @EventListener}</li>
 *   <li>{@link EventSubscriberRegistry} — discovers beans implementing {@code EventSubscriber} interface</li>
 *   <li>{@link SpringEventListenerRegistry} — Spring-aware annotation-based discovery</li>
 *   <li>{@link SpringEventSubscriberRegistry} — Spring-aware interface-based discovery</li>
 *   <li>{@link CompositeEventHandlerRegistry} — combines multiple registries</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Create registry
 * EventHandlerRegistry registry = new EventListenerRegistry();
 *
 * // Register handler
 * MyEventHandler handler = new MyEventHandler();
 * registry.register(handler);
 *
 * // Get handlers for specific event
 * MyEvent event = new MyEvent("data");
 * List<EventHandler> handlers = registry.getHandlers(event);
 *
 * // Invoke handlers
 * for (EventHandler handler : handlers) {
 *     handler.onEvent(event);
 * }
 * }</pre>
 * <p>
 * <b>Spring integration example:</b>
 * <pre>{@code
 * @Configuration
 * public class EventConfig {
 *
 *     @Bean
 *     public EventHandlerRegistry handlerRegistry(ApplicationContext context) {
 *         return new CompositeEventHandlerRegistry(List.of(
 *             new SpringEventListenerRegistry("com.example", context),
 *             new SpringEventSubscriberRegistry(context)
 *         ));
 *     }
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-07
 * @see EventHandler
 * @see EventSubscriber
 * @see Event
 * @see EventListenerRegistry
 * @see EventSubscriberRegistry
 * @see CompositeEventHandlerRegistry
 */
public interface EventHandlerRegistry {

    /**
     * Get all handlers that can handle the specified event type.
     * <p>
     * This method returns handlers registered for the exact event class.
     * Some implementations may also return handlers for parent event types
     * or generic {@code Event.class} handlers.
     *
     * @param event the event to find handlers for
     * @return list of handlers that handle this event type (never null)
     * @see EventSubscriber#events()
     */
    List<EventHandler> getHandlers(Event event);

    /**
     * Register an event handler in the registry.
     * <p>
     * The handler can be:
     * <ul>
     *   <li>An object with methods annotated with {@code @EventListener}</li>
     *   <li>An object implementing the {@code EventSubscriber} interface</li>
     * </ul>
     * <p>
     * Duplicate registrations are typically ignored or handled gracefully.
     *
     * @param eventHandler handler to register
     *                     — must implement EventSubscriber or have @EventListener methods
     * @throws InvalidEventListenerMethodSignatureException if a method has invalid signature
     * @see com.github.vovten.eventflow.EventListener
     */
    void register(Object eventHandler);

    /**
     * Unregister a handler from the registry.
     * <p>
     * Removes all registrations associated with the specified handler object.
     *
     * @param eventHandler the handler to unregister
     * @return true if the handler was found and removed, false if not registered
     */
    boolean unregister(Object eventHandler);

    /**
     * Check if a handler is currently registered in the registry.
     *
     * @param eventHandler the handler to check
     * @return true if registered, false otherwise
     */
    boolean isRegistered(Object eventHandler);

    /**
     * Merge another handler registry into this registry.
     * <p>
     * This operation allows combining handlers from multiple registries.
     * Not all implementations support this operation.
     *
     * @param registry the registry to merge
     * @throws UnsupportedOperationException if merging is not supported
     */
    void merge(EventHandlerRegistry registry);

    /**
     * Get the total number of registered handlers.
     * <p>
     * The count may represent:
     * <ul>
     *   <li>Number of handler objects</li>
     *   <li>Number of handler-method combinations</li>
     *   <li>Number of event type registrations</li>
     * </ul>
     * depending on the implementation.
     *
     * @return total number of registered handlers
     */
    int handlerCount();

    /**
     * @return human-readable identifier/name
     */
    String name();
}
