package com.github.vovten.eventflow.registry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for creating configured {@link EventListenerRegistry} instances.
 *
 * <p>
 * <b>Important rules:</b>
 * <ul>
 *   <li>When using Spring integration, scanPackage is REQUIRED</li>
 *   <li>scanPackage is only used with Spring-based registries</li>
 *   <li>Without Spring, scanPackage is ignored</li>
 * </ul>
 *
 * <p>
 * <b>Usage examples:</b>
 * <pre>
 * // Simple annotation-based registry (non-Spring)
 * EventListenerRegistry registry = EventListenerRegistryBuilder.create()
 *     .withAnnotationListeners()
 *     .build();
 *
 * // Spring-based registry with package scan
 * EventListenerRegistry registry = EventListenerRegistryBuilder.create()
 *     .withSpring(applicationContext, "com.example.listeners")  // package is REQUIRED
 *     .withAnnotationListeners()
 *     .withInterfaceListeners()
 *     .build();
 *
 * // Composite registry with custom registries and decorators
 * EventListenerRegistry registry = EventListenerRegistryBuilder.create()
 *     .withAnnotationListeners()
 *     .withInterfaceListeners()
 *     .withRegistry(customRegistry)
 *     .withDecorator(loggingDecorator)
 *     .build();
 * </pre>
 */
@Slf4j
public class EventListenerRegistryBuilder {

    private String scanPackage;
    private ApplicationContext springContext;
    private boolean useInterfaceListeners = false;
    private boolean useAnnotationListeners = false;

    private final List<DecoratorFunction> decorators = new ArrayList<>();
    private final List<EventListenerRegistry> additionalRegistries = new ArrayList<>();

    private EventListenerRegistryBuilder() {
    }

    /**
     * Start building a new EventListenerRegistry.
     */
    public static EventListenerRegistryBuilder create() {
        return new EventListenerRegistryBuilder();
    }

    /**
     * Enable Spring integration with application context and package scan.
     * <p>
     * <b>IMPORTANT:</b> Package scan is REQUIRED when using Spring.
     *
     * @param context       Spring application context
     * @param packageToScan base package to scan for listeners (e.g., "com.example.listeners")
     * @return this builder
     * @throws IllegalArgumentException if context is null or package is empty
     */
    public EventListenerRegistryBuilder withSpring(ApplicationContext context, String packageToScan) {
        if (context == null) {
            throw new IllegalArgumentException("Spring context cannot be null");
        }
        if (packageToScan == null || packageToScan.isEmpty()) {
            throw new IllegalArgumentException(
                    "Scan package is REQUIRED when using Spring. " +
                            "Please provide a valid package name (e.g., 'com.example.listeners')"
            );
        }

        this.springContext = context;
        this.scanPackage = packageToScan;
        return this;
    }

    /**
     * Include listeners based on annotations (e.g., @EventListener).
     */
    public EventListenerRegistryBuilder withAnnotationListeners() {
        this.useAnnotationListeners = true;
        return this;
    }

    /**
     * Include listeners based on interface implementation (e.g., EventListener interface).
     */
    public EventListenerRegistryBuilder withInterfaceListeners() {
        this.useInterfaceListeners = true;
        return this;
    }

    /**
     * Add a custom registry to the composite.
     */
    public EventListenerRegistryBuilder withCustomRegistry(EventListenerRegistry registry) {
        if (registry != null) {
            this.additionalRegistries.add(registry);
        }
        return this;
    }

    /**
     * Add a decorator to wrap the registry.
     */
    public EventListenerRegistryBuilder withDecorator(DecoratorFunction decorator) {
        if (decorator != null) {
            this.decorators.add(decorator);
        }
        return this;
    }

    /**
     * Build the registry without logging.
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
     */
    public EventListenerRegistry buildAndLog() {
        EventListenerRegistry registry = build();

        log.info("Built EventListenerRegistry: springContext={}, scanPackage='{}', " +
                        "annotationListeners={}, interfaceListeners={}, customRegistries={}, decorators={}",
                springContext != null ? "yes" : "no",
                scanPackage != null ? scanPackage : "N/A",
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
                            Use withAnnotationListeners(), withInterfaceListeners(), or withRegistry()."""
            );
        }

        // If Spring is used, scan package must be provided
        if (springContext != null && scanPackage == null) {
            throw new IllegalStateException(
                    """
                            Scan package is REQUIRED when using Spring context.
                            Please use: .withSpring(context, "your.base.package")
                            Example: .withSpring(applicationContext, "com.example.listeners")"""
            );
        }
    }

    private EventListenerRegistry createRegistry() {
        List<EventListenerRegistry> registries = new ArrayList<>();

        // Add annotation-based registry if requested
        if (useAnnotationListeners) {
            registries.add(createAnnotationRegistry());
        }

        // Add interface-based registry if requested
        if (useInterfaceListeners) {
            registries.add(createInterfaceRegistry());
        }

        // Add custom registries
        registries.addAll(additionalRegistries);

        // Return single registry or composite
        if (registries.size() == 1) {
            return registries.getFirst();
        }

        return new CompositeEventListenerRegistry(registries);
    }

    private EventListenerRegistry createAnnotationRegistry() {
        if (springContext != null) {
            SpringAnnotationEventListenerRegistry registry =
                    new SpringAnnotationEventListenerRegistry(springContext, scanPackage);
            registerInSpring(registry, "springAnnotationEventListenerRegistry");
            return registry;
        }

        return new AnnotationEventListenerRegistry();
    }

    private EventListenerRegistry createInterfaceRegistry() {
        if (springContext != null) {
            SpringInterfaceEventListenerRegistry registry =
                    new SpringInterfaceEventListenerRegistry(springContext);
            registerInSpring(registry, "springInterfaceEventListenerRegistry");
            return registry;
        }

        return new InterfaceEventListenerRegistry();
    }

    private void registerInSpring(Object registry, String beanName) {
        if (springContext instanceof ConfigurableApplicationContext configurableContext) {
            ConfigurableListableBeanFactory factory = configurableContext.getBeanFactory();

            if (!factory.containsSingleton(beanName)) {
                factory.autowireBean(registry);
                factory.initializeBean(registry, beanName);
                factory.registerSingleton(beanName, registry);
                log.debug("Registered {} as Spring bean", beanName);
            }
        }
    }

    // ==================== Inner types ====================

    @FunctionalInterface
    public interface DecoratorFunction {
        EventListenerRegistry apply(EventListenerRegistry registry);
    }
}