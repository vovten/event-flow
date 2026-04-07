package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Spring-aware fluent builder for creating configured {@link EventHandlerRegistry} instances.
 * <p>
 * Extends the core {@link EventHandlerRegistryBuilder} with Spring-specific handler discovery:
 * <ul>
 *   <li>{@link SpringEventListenerRegistry} — methods annotated with {@code @EventListener}</li>
 *   <li>{@link SpringEventSubscriberRegistry} — beans implementing {@link EventSubscriber} interface</li>
 * </ul>
 * <p>
 * <b>Usage examples:</b>
 * <pre>
 * // Simple annotation-based registry
 * EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(context)
 *     .withAnnotationListeners("com.example")
 *     .build();
 *
 * // Interface-based registry
 * EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(context)
 *     .withInterfaceListeners()
 *     .build();
 *
 * // Composite registry with multiple strategies
 * EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(context)
 *     .withAnnotationListeners("com.example")
 *     .withInterfaceListeners()
 *     .withCustomRegistry(customRegistry)
 *     .withDecorator(loggingDecorator)
 *     .build();
 * </pre>
 * <p>
 * <b>Package scanning:</b>
 * For annotation-based listeners, the {@code scanPackage} parameter controls which beans are scanned:
 * <ul>
 *   <li>"com.example" — beans in com.example and subpackages</li>
 *   <li>"com.example.service" — beans in com.example.service only</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-31
 * @see EventHandlerRegistryBuilder
 * @see SpringEventListenerRegistry
 * @see SpringEventSubscriberRegistry
 */
public final class SpringEventHandlerRegistryBuilder extends EventHandlerRegistryBuilder<SpringEventHandlerRegistryBuilder> {

    private static final Logger log = LoggerFactory.getLogger(SpringEventHandlerRegistryBuilder.class);

    private final ApplicationContext applicationContext;

    private String scanPackage = null;

    private SpringEventHandlerRegistryBuilder(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            throw new IllegalArgumentException("ApplicationContext is required");
        }
        this.applicationContext = applicationContext;
    }

    /**
     * Start building a new Spring-aware EventHandlerRegistry.
     *
     * @param applicationContext Spring application context (required)
     * @return new builder instance
     */
    public static SpringEventHandlerRegistryBuilder create(ApplicationContext applicationContext) {
        return new SpringEventHandlerRegistryBuilder(applicationContext);
    }

    /**
     * Include handlers based on annotations (e.g., {@code @EventListener}).
     * <p>
     * Uses {@link SpringEventListenerRegistry} to discover methods
     * annotated with the {@code @EventListener} annotation.
     *
     * @param scanPackage package prefix for filtering beans (e.g., "com.example")
     * @return this builder
     * @throws IllegalArgumentException if scanPackage is null or empty
     */
    public SpringEventHandlerRegistryBuilder withAnnotationListeners(String scanPackage) {
        if (scanPackage == null || scanPackage.isEmpty()) {
            throw new IllegalArgumentException("Scan package must not be null or empty");
        }
        this.scanPackage = scanPackage;
        return this;
    }

    /**
     * Include handlers based on interface implementation (e.g., {@link com.github.vovten.eventflow.EventSubscriber} interface).
     * <p>
     * Uses {@link SpringEventSubscriberRegistry} to discover beans that implement
     * the {@code EventSubscriber} interface.
     *
     * @return this builder
     */
    public SpringEventHandlerRegistryBuilder withInterfaceListeners() {
        this.useInterfaceListeners = true;
        return this;
    }

    @Override
    protected void validateConfiguration() {
        // Must have at least one handler source
        if (scanPackage == null && !useInterfaceListeners && additionalRegistries.isEmpty()) {
            throw new IllegalStateException(
                    """
                            At least one handler source must be configured.
                            Use withAnnotationListeners(package), withInterfaceListeners(), or withCustomRegistry()."""
            );
        }
    }

    @Override
    protected EventHandlerRegistry createRegistry() {
        List<EventHandlerRegistry> registries = new ArrayList<>();

        if (scanPackage != null) {
            registries.add(createEventListenerRegistry(scanPackage));
        }

        if (useInterfaceListeners) {
            registries.add(createEventSubscriberRegistry());
        }

        registries.addAll(additionalRegistries);

        if (registries.size() == 1) {
            return registries.getFirst();
        }

        return new CompositeEventHandlerRegistry(registries);
    }

    private SpringEventListenerRegistry createEventListenerRegistry(String scanPackage) {
        return getOrCreateBean(
                SpringEventListenerRegistry.class,
                "springEventListenerRegistry",
                () -> new SpringEventListenerRegistry(applicationContext, scanPackage)
        );
    }

    private SpringEventSubscriberRegistry createEventSubscriberRegistry() {
        return getOrCreateBean(
                SpringEventSubscriberRegistry.class,
                "springEventSubscriberRegistry",
                () -> new SpringEventSubscriberRegistry(applicationContext)
        );
    }

    private <T> T getOrCreateBean(Class<T> beanClass, String beanName, Supplier<T> supplier) {
        if (applicationContext instanceof GenericApplicationContext ctx) {
            if (!ctx.containsBean(beanName)) {
                ctx.registerBean(beanName, beanClass, supplier);
            }
            return ctx.getBean(beanName, beanClass);
        }
        throw new IllegalStateException("Context must be an instance of GenericApplicationContext");
    }

    @Override
    public EventHandlerRegistry buildAndLog() {
        EventHandlerRegistry registry = build();

        log.info("Built SpringEventHandlerRegistry: annotationListeners={}, " +
                        "interfaceListeners={}, customRegistries={}, decorators={}",
                scanPackage != null ? "enabled (" + scanPackage + ")" : "disabled",
                useInterfaceListeners,
                additionalRegistries.size(),
                decorators.size()
        );

        return registry;
    }
}
