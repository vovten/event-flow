package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.EventSubscriber;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for creating configured {@link EventHandlerRegistry} instances.
 *
 * <p>
 * This builder supports only non-Spring handler discovery strategies:
 * <ul>
 *   <li>{@link EventListenerRegistry} — methods annotated with {@code @EventListener}</li>
 *   <li>{@link EventSubscriberRegistry} — beans implementing {@link EventSubscriber} interface</li>
 *   <li>Custom registries via {@link #withCustomRegistry(EventHandlerRegistry)}</li>
 * </ul>
 *
 * <p>
 * <b>Usage examples:</b>
 * <pre>
 * // Simple annotation-based registry
 * EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
 *     .withAnnotationListeners()
 *     .build();
 *
 * // Interface-based registry
 * EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
 *     .withInterfaceListeners()
 *     .build();
 *
 * // Composite registry with multiple strategies
 * EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
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
public class EventHandlerRegistryBuilder {

    protected boolean useInterfaceListeners = false;
    protected boolean useAnnotationListeners = false;

    protected final List<DecoratorFunction> decorators = new ArrayList<>();
    protected final List<EventHandlerRegistry> additionalRegistries = new ArrayList<>();

    protected EventHandlerRegistryBuilder() {
    }

    /**
     * Start building a new EventHandlerRegistry.
     *
     * @return new builder instance
     */
    public static EventHandlerRegistryBuilder create() {
        return new EventHandlerRegistryBuilder();
    }

    /**
     * Include handlers based on annotations (e.g., {@code @EventListener}).
     * <p>
     * Uses {@link EventListenerRegistry} to discover methods
     * annotated with the {@code @EventListener} annotation.
     *
     * @return this builder
     */
    public EventHandlerRegistryBuilder withAnnotationListeners() {
        this.useAnnotationListeners = true;
        return this;
    }

    /**
     * Include handlers based on interface implementation (e.g., {@link EventSubscriber} interface).
     * <p>
     * Uses {@link EventSubscriberRegistry} to discover beans that implement
     * the {@code EventSubscriber} interface.
     *
     * @return this builder
     */
    public EventHandlerRegistryBuilder withInterfaceListeners() {
        this.useInterfaceListeners = true;
        return this;
    }

    /**
     * Add a custom registry to the composite.
     *
     * @param registry custom registry to add
     * @return this builder
     */
    public EventHandlerRegistryBuilder withCustomRegistry(EventHandlerRegistry registry) {
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
    public EventHandlerRegistryBuilder withDecorator(DecoratorFunction decorator) {
        if (decorator != null) {
            this.decorators.add(decorator);
        }
        return this;
    }

    /**
     * Build the registry without logging.
     *
     * @return configured EventHandlerRegistry instance
     */
    public EventHandlerRegistry build() {
        validateConfiguration();
        EventHandlerRegistry registry = createRegistry();

        for (DecoratorFunction decorator : decorators) {
            registry = decorator.apply(registry);
        }

        return registry;
    }

    /**
     * Build the registry and log the configuration.
     *
     * @return configured EventHandlerRegistry instance
     */
    public EventHandlerRegistry buildAndLog() {
        EventHandlerRegistry registry = build();

        log.info("Built EventHandlerRegistry: annotationListeners={}, " +
                        "interfaceListeners={}, customRegistries={}, decorators={}",
                useAnnotationListeners,
                useInterfaceListeners,
                additionalRegistries.size(),
                decorators.size()
        );

        return registry;
    }

    // ==================== Private methods ====================

    /**
     * Validate the builder configuration.
     *
     * @throws IllegalStateException if no handler source is configured
     */
    protected void validateConfiguration() {
        // Must have at least one handler source
        if (!useAnnotationListeners && !useInterfaceListeners && additionalRegistries.isEmpty()) {
            throw new IllegalStateException(
                    """
                            At least one handler source must be configured.
                            Use withAnnotationListeners(), withInterfaceListeners(), or withCustomRegistry()."""
            );
        }
    }

    /**
     * Create the registry instance based on the current configuration.
     * <p>
     * Subclasses can override this method to provide custom registry implementations.
     *
     * @return configured EventHandlerRegistry
     */
    protected EventHandlerRegistry createRegistry() {
        List<EventHandlerRegistry> registries = new ArrayList<>();

        // Add annotation-based registry if requested
        if (useAnnotationListeners) {
            registries.add(new EventListenerRegistry());
        }

        // Add interface-based registry if requested
        if (useInterfaceListeners) {
            registries.add(new EventSubscriberRegistry());
        }

        // Add custom registries
        registries.addAll(additionalRegistries);

        // Return single registry or composite
        if (registries.size() == 1) {
            return registries.getFirst();
        }

        return new CompositeEventHandlerRegistry(registries);
    }

    // ==================== Inner types ====================

    /**
     * Functional interface for decorating event handler registries.
     */
    @FunctionalInterface
    public interface DecoratorFunction {
        /**
         * Apply a decorator to the given registry.
         *
         * @param registry the registry to decorate
         * @return decorated registry
         */
        EventHandlerRegistry apply(EventHandlerRegistry registry);
    }
}
