package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;

import java.util.List;

/**
 * Composite registry that combines multiple {@link EventListenerRegistry} implementations.
 * <p>
 * This class implements the Composite pattern, allowing multiple registries to be treated
 * as a single registry. It delegates all operations to its constituent registries,
 * aggregating their results.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Combines multiple listener discovery strategies</li>
 *   <li>Aggregates listeners from all child registries</li>
 *   <li>Propagates registration/unregistration to all children</li>
 *   <li>Sums listener counts across all registries</li>
 * </ul>
 * <p>
 * <b>Use cases:</b>
 * <ul>
 *   <li>Support both annotation-based and interface-based listeners</li>
 *   <li>Combine Spring-managed and manually registered listeners</li>
 *   <li>Enable modular listener configuration</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>{@code
 * CompositeEventListenerRegistry
 * ├── AnnotationEventListenerRegistry (methods with @EventListener)
 * ├── InterfaceEventListenerRegistry (beans implementing EventListener)
 * └── SpringAnnotationEventListenerRegistry (Spring beans with @EventListener)
 * }</pre>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Create composite registry with multiple strategies
 * EventListenerRegistry registry = new CompositeEventListenerRegistry(List.of(
 *     new AnnotationEventListenerRegistry(),
 *     new InterfaceEventListenerRegistry()
 * ));
 *
 * // Register listeners — they go to all child registries
 * registry.register(new MyAnnotationListener());
 * registry.register(new MyInterfaceListener());
 *
 * // Get listeners — aggregates from all children
 * List<EventListener> allListeners = registry.getListeners(event);
 * }</pre>
 * <p>
 * <b>Spring configuration example:</b>
 * <pre>{@code
 * @Bean
 * public EventListenerRegistry eventListenerRegistry(
 *         SpringAnnotationEventListenerRegistry annotationRegistry,
 *         SpringInterfaceEventListenerRegistry interfaceRegistry) {
 *     return new CompositeEventListenerRegistry(
 *         List.of(annotationRegistry, interfaceRegistry)
 *     );
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-07
 * @see EventListenerRegistry
 * @see AnnotationEventListenerRegistry
 * @see InterfaceEventListenerRegistry
 */
public class CompositeEventListenerRegistry implements EventListenerRegistry {

    private final List<EventListenerRegistry> registries;

    /**
     * Creates a composite registry from the specified list of registries.
     * <p>
     * The registries are queried in order when retrieving listeners.
     * Registration operations are propagated to all registries.
     *
     * @param registries list of child registries to combine
     * @throws IllegalArgumentException if registries is null or empty
     */
    public CompositeEventListenerRegistry(List<EventListenerRegistry> registries) {
        if (registries == null || registries.isEmpty()) {
            throw new IllegalArgumentException("At least one registry must be provided");
        }
        this.registries = registries;
    }

    /**
     * Get all listeners from all child registries for the specified event.
     * <p>
     * This method aggregates listeners from all constituent registries,
     * preserving the order in which registries were provided.
     *
     * @param event the event to find listeners for
     * @return combined list of listeners from all registries
     */
    @Override
    public List<EventListener> getListeners(Event event) {
        return registries.stream()
                .flatMap(registry -> registry.getListeners(event).stream())
                .toList();
    }

    /**
     * Get total number of listeners across all child registries.
     *
     * @return sum of listener counts from all registries
     */
    @Override
    public int listenerCount() {
        return registries.stream()
                .mapToInt(EventListenerRegistry::listenerCount)
                .sum();
    }

    /**
     * Register a listener in all child registries.
     * <p>
     * The listener is propagated to each registry in order.
     * If any registry throws an exception, registration stops.
     *
     * @param eventListener listener to register
     */
    @Override
    public void register(Object eventListener) {
        registries.forEach(registry -> registry.register(eventListener));
    }

    /**
     * Unregister a listener from all child registries.
     * <p>
     * Returns true if the listener was removed from at least one registry.
     *
     * @param eventListener listener to unregister
     * @return true if removed from any registry, false otherwise
     */
    @Override
    public boolean unregister(Object eventListener) {
        return registries.stream()
                .map(registry -> registry.unregister(eventListener))
                .reduce(false, (a, b) -> a || b);
    }

    /**
     * Check if a listener is registered in any child registry.
     *
     * @param eventListener listener to check
     * @return true if found in any registry, false otherwise
     */
    @Override
    public boolean isRegistered(Object eventListener) {
        return registries.stream().anyMatch(registry -> registry.isRegistered(eventListener));
    }

    /**
     * Merge another registry into this composite.
     * <p>
     * If the specified registry is also a composite, all its children are added.
     * Otherwise, the registry itself is added as a child.
     *
     * @param registry the registry to merge
     */
    @Override
    public void merge(EventListenerRegistry registry) {
        if (registry instanceof CompositeEventListenerRegistry composite) {
            this.registries.addAll(composite.registries);
        } else {
            this.registries.add(registry);
        }
    }
}
