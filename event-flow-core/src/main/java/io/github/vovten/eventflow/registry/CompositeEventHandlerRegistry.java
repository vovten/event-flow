package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventHandler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Composite registry that combines multiple {@link EventHandlerRegistry} implementations.
 * <p>
 * This class implements the Composite pattern, allowing multiple registries to be treated
 * as a single registry. It delegates all operations to its constituent registries,
 * aggregating their results.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Combines multiple handler discovery strategies</li>
 *   <li>Aggregates handlers from all child registries</li>
 *   <li>Propagates registration/unregistration to all children</li>
 *   <li>Sums handler counts across all registries</li>
 * </ul>
 * <p>
 * <b>Use cases:</b>
 * <ul>
 *   <li>Support both annotation-based and interface-based handlers</li>
 *   <li>Combine Spring-managed and manually registered handlers</li>
 *   <li>Enable modular handler configuration</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>{@code
 * CompositeEventHandlerRegistry
 * ├── EventListenerRegistry (methods with @EventListener)
 * ├── EventSubscriberRegistry (beans implementing EventSubscriber)
 * └── SpringEventListenerRegistry (Spring beans with @EventListener)
 * }</pre>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Create composite registry with multiple strategies
 * EventHandlerRegistry registry = new CompositeEventHandlerRegistry(List.of(
 *     new EventListenerRegistry(),
 *     new EventSubscriberRegistry()
 * ));
 *
 * // Register handlers — they go to all child registries
 * registry.register(new MyAnnotationHandler());
 * registry.register(new MyInterfaceHandler());
 *
 * // Get handlers — aggregates from all children
 * List<EventHandler> allHandlers = registry.getHandlers(event);
 * }</pre>
 * <p>
 * <b>Spring configuration example:</b>
 * <pre>{@code
 * @Bean
 * public EventHandlerRegistry handlerRegistry(
 *         SpringEventListenerRegistry annotationRegistry,
 *         SpringEventSubscriberRegistry interfaceRegistry) {
 *     return new CompositeEventHandlerRegistry(
 *         List.of(annotationRegistry, interfaceRegistry)
 *     );
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-07
 * @see EventHandlerRegistry
 * @see EventListenerRegistry
 * @see EventSubscriberRegistry
 */
public class CompositeEventHandlerRegistry implements EventHandlerRegistry {

    /**
     * List of child registries. Thread-safe for concurrent access and modification.
     */
    private final List<EventHandlerRegistry> registries;

    /**
     * Creates a composite registry from the specified list of registries.
     * <p>
     * The registries are queried in order when retrieving handlers.
     * Registration operations are propagated to all registries.
     * <p>
     * Thread-safe: creates a defensive copy using CopyOnWriteArrayList.
     *
     * @param registries list of child registries to combine
     * @throws IllegalArgumentException if registries is null or empty
     */
    public CompositeEventHandlerRegistry(List<EventHandlerRegistry> registries) {
        if (registries == null || registries.isEmpty()) {
            throw new IllegalArgumentException("At least one registry must be provided");
        }
        this.registries = new CopyOnWriteArrayList<>(registries);
    }

    /**
     * Get all handlers from all child registries for the specified event.
     * <p>
     * This method aggregates handlers from all constituent registries,
     * preserving the order in which registries were provided.
     *
     * @param event the event to find handlers for
     * @return combined list of handlers from all registries
     */
    @Override
    public List<EventHandler> getHandlers(Event event) {
        return registries.stream()
                .flatMap(registry -> registry.getHandlers(event).stream())
                .toList();
    }

    /**
     * Get total number of handlers across all child registries.
     *
     * @return sum of handler counts from all registries
     */
    @Override
    public int handlerCount() {
        return registries.stream()
                .mapToInt(EventHandlerRegistry::handlerCount)
                .sum();
    }

    /**
     * Register a handler in all child registries.
     * <p>
     * The handler is propagated to each registry in order.
     * If any registry throws an exception, registration stops.
     *
     * @param eventHandler handler to register
     */
    @Override
    public void register(Object eventHandler) {
        registries.forEach(registry -> registry.register(eventHandler));
    }

    /**
     * Unregister a handler from all child registries.
     * <p>
     * Returns true if the handler was removed from at least one registry.
     *
     * @param eventHandler handler to unregister
     * @return true if removed from any registry, false otherwise
     */
    @Override
    public boolean unregister(Object eventHandler) {
        return registries.stream()
                .map(registry -> registry.unregister(eventHandler))
                .reduce(false, (a, b) -> a || b);
    }

    /**
     * Check if a handler is registered in any child registry.
     *
     * @param eventHandler handler to check
     * @return true if found in any registry, false otherwise
     */
    @Override
    public boolean isRegistered(Object eventHandler) {
        return registries.stream().anyMatch(registry -> registry.isRegistered(eventHandler));
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
    public void merge(EventHandlerRegistry registry) {
        if (registry instanceof CompositeEventHandlerRegistry composite) {
            this.registries.addAll(composite.registries);
        } else {
            this.registries.add(registry);
        }
    }

    @Override
    public String name() {
        return "composite[" + registries.stream()
                .map(EventHandlerRegistry::name)
                .collect(java.util.stream.Collectors.joining(",")) + "]";
    }
}
