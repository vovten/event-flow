package com.github.vovten.eventflow.registry;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for creating configured {@link EventListenerRegistry} instances.
 *
 * <p>
 * This builder supports only non-Spring listener discovery strategies:
 * <ul>
 *   <li>{@link AnnotationEventListenerRegistry} — methods annotated with {@code @EventListener}</li>
 *   <li>{@link InterfaceEventListenerRegistry} — beans implementing {@link EventListener} interface</li>
 *   <li>Custom registries via {@link #withCustomRegistry(EventListenerRegistry)}</li>
 * </ul>
 *
 * <p>
 * <b>Usage examples:</b>
 * <pre>
 * // Simple annotation-based registry
 * EventListenerRegistry registry = EventListenerRegistryBuilder.create()
 *     .withAnnotationListeners()
 *     .build();
 *
 * // Interface-based registry
 * EventListenerRegistry registry = EventListenerRegistryBuilder.create()
 *     .withInterfaceListeners()
 *     .build();
 *
 * // Composite registry with multiple strategies
 * EventListenerRegistry registry = EventListenerRegistryBuilder.create()
 *     .withAnnotationListeners()
 *     .withInterfaceListeners()
 *     .withCustomRegistry(customRegistry)
 *     .withDecorator(loggingDecorator)
 *     .build();
 * </pre>
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-07
 */
@Slf4j
public final class EventListenerRegistryBuilder {

    private boolean useInterfaceListeners = false;
    private boolean useAnnotationListeners = false;

    private final List<DecoratorFunction> decorators = new ArrayList<>();
    private final List<EventListenerRegistry> additionalRegistries = new ArrayList<>();

    private EventListenerRegistryBuilder() {
    }

    /**
     * Start building a new EventListenerRegistry.
     *
     * @return new builder instance
     */
    public static EventListenerRegistryBuilder create() {
        return new EventListenerRegistryBuilder();
    }

    /**
     * Include listeners based on annotations (e.g., {@code @EventListener}).
     * <p>
     * Uses {@link AnnotationEventListenerRegistry} to discover methods
     * annotated with the {@code @EventListener} annotation.
     *
     * @return this builder
     */
    public EventListenerRegistryBuilder withAnnotationListeners() {
        this.useAnnotationListeners = true;
        return this;
    }

    /**
     * Include listeners based on interface implementation (e.g., {@link EventListener} interface).
     * <p>
     * Uses {@link InterfaceEventListenerRegistry} to discover beans that implement
     * the {@code EventListener} interface.
     *
     * @return this builder
     */
    public EventListenerRegistryBuilder withInterfaceListeners() {
        this.useInterfaceListeners = true;
        return this;
    }

    /**
     * Add a custom registry to the composite.
     *
     * @param registry custom registry to add
     * @return this builder
     */
    public EventListenerRegistryBuilder withCustomRegistry(EventListenerRegistry registry) {
        if (registry != null) {
            this.additionalRegistries.add(registry);
        }
        return this;
    }

    /**
     * Add a decorator to wrap the registry.
     *
     * @param decorator decorator function to apply
     * @return this builder
     */
    public EventListenerRegistryBuilder withDecorator(DecoratorFunction decorator) {
        if (decorator != null) {
            this.decorators.add(decorator);
        }
        return this;
    }

    /**
     * Build the registry without logging.
     *
     * @return configured EventListenerRegistry instance
     */
    public EventListenerRegistry build() {
        validateConfiguration();
        EventListenerRegistry registry = createRegistry();

        for (DecoratorFunction decorator : decorators) {
            registry = decorator.apply(registry);
        }

        return registry;
    }

    /**
     * Build the registry and log the configuration.
     *
     * @return configured EventListenerRegistry instance
     */
    public EventListenerRegistry buildAndLog() {
        EventListenerRegistry registry = build();

        log.info("Built EventListenerRegistry: annotationListeners={}, " +
                        "interfaceListeners={}, customRegistries={}, decorators={}",
                useAnnotationListeners,
                useInterfaceListeners,
                additionalRegistries.size(),
                decorators.size()
        );

        return registry;
    }

    // ==================== Private methods ====================

    private void validateConfiguration() {
        // Must have at least one listener source
        if (!useAnnotationListeners && !useInterfaceListeners && additionalRegistries.isEmpty()) {
            throw new IllegalStateException(
                    """
                            At least one listener source must be configured.
                            Use withAnnotationListeners(), withInterfaceListeners(), or withCustomRegistry()."""
            );
        }
    }

    private EventListenerRegistry createRegistry() {
        List<EventListenerRegistry> registries = new ArrayList<>();

        // Add annotation-based registry if requested
        if (useAnnotationListeners) {
            registries.add(new AnnotationEventListenerRegistry());
        }

        // Add interface-based registry if requested
        if (useInterfaceListeners) {
            registries.add(new InterfaceEventListenerRegistry());
        }

        // Add custom registries
        registries.addAll(additionalRegistries);

        // Return single registry or composite
        if (registries.size() == 1) {
            return registries.getFirst();
        }

        return new CompositeEventListenerRegistry(registries);
    }

    // ==================== Inner types ====================

    /**
     * Functional interface for decorating event listener registries.
     */
    @FunctionalInterface
    public interface DecoratorFunction {
        /**
         * Apply a decorator to the given registry.
         *
         * @param registry the registry to decorate
         * @return decorated registry
         */
        EventListenerRegistry apply(EventListenerRegistry registry);
    }
}
