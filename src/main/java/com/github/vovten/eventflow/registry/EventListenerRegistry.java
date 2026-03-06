package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;

import java.util.List;

/**
 * Registry for managing event listeners.
 * <p>
 * The registry is responsible for storing, discovering, and retrieving event listeners
 * that should be notified when specific events are published. It provides a centralized
 * mechanism for listener management within the event dispatching system.
 * <p>
 * <b>Key responsibilities:</b>
 * <ul>
 *   <li>Discover and register event listeners from various sources</li>
 *   <li>Match listeners to events based on event type</li>
 *   <li>Support dynamic registration and unregistration of listeners</li>
 *   <li>Enable composition of multiple registry implementations</li>
 * </ul>
 * <p>
 * <b>Listener discovery strategies:</b>
 * <ul>
 *   <li>{@link AnnotationEventListenerRegistry} — discovers methods annotated with {@code @EventListener}</li>
 *   <li>{@link InterfaceEventListenerRegistry} — discovers beans implementing {@code EventListener} interface</li>
 *   <li>{@link SpringAnnotationEventListenerRegistry} — Spring-aware annotation-based discovery</li>
 *   <li>{@link SpringInterfaceEventListenerRegistry} — Spring-aware interface-based discovery</li>
 *   <li>{@link CompositeEventListenerRegistry} — combines multiple registries</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Create registry
 * EventListenerRegistry registry = new AnnotationEventListenerRegistry();
 *
 * // Register listener
 * MyEventListener listener = new MyEventListener();
 * registry.register(listener);
 *
 * // Get listeners for specific event
 * MyEvent event = new MyEvent("data");
 * List<EventListener> listeners = registry.getListeners(event);
 *
 * // Invoke listeners
 * for (EventListener listener : listeners) {
 *     listener.onEvent(event);
 * }
 * }</pre>
 * <p>
 * <b>Spring integration example:</b>
 * <pre>{@code
 * @Configuration
 * public class EventConfig {
 *
 *     @Bean
 *     public EventListenerRegistry listenerRegistry(ApplicationContext context) {
 *         return new CompositeEventListenerRegistry(List.of(
 *             new SpringAnnotationEventListenerRegistry("com.example", context),
 *             new SpringInterfaceEventListenerRegistry(context)
 *         ));
 *     }
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 07.12.2024
 * @see EventListener
 * @see Event
 * @see AnnotationEventListenerRegistry
 * @see InterfaceEventListenerRegistry
 * @see CompositeEventListenerRegistry
 */
public interface EventListenerRegistry {

    /**
     * Get all listeners that can handle the specified event type.
     * <p>
     * This method returns listeners registered for the exact event class.
     * Some implementations may also return listeners for parent event types
     * or generic {@code Event.class} listeners.
     *
     * @param event the event to find listeners for
     * @return list of listeners that handle this event type (never null)
     * @see EventListener#events()
     */
    List<EventListener> getListeners(Event event);

    /**
     * Register an event listener in the registry.
     * <p>
     * The listener can be:
     * <ul>
     *   <li>An object with methods annotated with {@code @EventListener}</li>
     *   <li>An object implementing the {@code EventListener} interface</li>
     * </ul>
     * <p>
     * Duplicate registrations are typically ignored or handled gracefully.
     *
     * @param eventListener listener to register
     *                     — must implement EventListener or have @EventListener methods
     * @throws InvalidEventListenerMethodSignatureException if a method has invalid signature
     * @see com.github.vovten.eventflow.annotation.EventListener
     */
    void register(Object eventListener);

    /**
     * Unregister a listener from the registry.
     * <p>
     * Removes all registrations associated with the specified listener object.
     *
     * @param eventListener the listener to unregister
     * @return true if the listener was found and removed, false if not registered
     */
    boolean unregister(Object eventListener);

    /**
     * Check if a listener is currently registered in the registry.
     *
     * @param eventListener the listener to check
     * @return true if registered, false otherwise
     */
    boolean isRegistered(Object eventListener);

    /**
     * Merge another listener registry into this registry.
     * <p>
     * This operation allows combining listeners from multiple registries.
     * Not all implementations support this operation.
     *
     * @param registry the registry to merge
     * @throws UnsupportedOperationException if merging is not supported
     */
    void merge(EventListenerRegistry registry);

    /**
     * Get the total number of registered listeners.
     * <p>
     * The count may represent:
     * <ul>
     *   <li>Number of listener objects</li>
     *   <li>Number of listener-method combinations</li>
     *   <li>Number of event type registrations</li>
     * </ul>
     * depending on the implementation.
     *
     * @return total number of registered listeners
     */
    int listenerCount();
}
