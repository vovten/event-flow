package com.github.vovten.eventflow.registry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for creating configured {@link EventListenerRegistry} instances.
 * <p>
 * <b>Important rules:</b>
 * <ul>
 *   <li>When using Spring integration, scanPackage is REQUIRED</li>
 *   <li>scanPackage is only used with Spring-based registries</li>
 *   <li>Without Spring, scanPackage is ignored</li>
 * </ul>
 */
@Slf4j
public class EventListenerRegistryBuilder {

    private ApplicationContext springContext;
    private String scanPackage = "";
    private RegistryType registryType = RegistryType.COMPOSITE;

    private boolean includeAnnotationListeners = false;
    private boolean includeInterfaceListeners = false;

    private final List<DecoratorFunction> decorators = new ArrayList<>();
    private final List<EventListenerRegistry> additionalRegistries = new ArrayList<>();

    private EventListenerRegistryBuilder() {}

    private EventListenerRegistryBuilder(RegistryType registryType) {
        this.registryType = registryType;
    }

    // ==================== Static factories ====================

    public static EventListenerRegistryBuilder annotationBased() {
        return new EventListenerRegistryBuilder(RegistryType.ANNOTATION);
    }

    public static EventListenerRegistryBuilder interfaceBased() {
        return new EventListenerRegistryBuilder(RegistryType.INTERFACE);
    }

    public static EventListenerRegistryBuilder composite() {
        return new EventListenerRegistryBuilder(RegistryType.COMPOSITE)
                .includeAnnotationListeners()
                .includeInterfaceListeners();
    }

    public static EventListenerRegistryBuilder spring() {
        return new EventListenerRegistryBuilder(RegistryType.COMPOSITE)
                .includeAnnotationListeners()
                .includeInterfaceListeners();
    }

    public static EventListenerRegistryBuilder springAnnotationBased() {
        return new EventListenerRegistryBuilder(RegistryType.SPRING_ANNOTATION);
    }

    public static EventListenerRegistryBuilder springInterfaceBased() {
        return new EventListenerRegistryBuilder(RegistryType.SPRING_INTERFACE);
    }

    // ==================== Configuration ====================

    /**
     * Set Spring application context.
     * <p>
     * <b>IMPORTANT:</b> When using Spring, you MUST also call {@link #scanPackage(String)}
     * to specify which package to scan for listeners.
     */
    public EventListenerRegistryBuilder withSpringContext(ApplicationContext context) {
        this.springContext = context;
        return this;
    }

    /**
     * Set the package to scan for listeners.
     * <p>
     * <b>REQUIRED when using Spring integration.</b>
     * Without Spring, this parameter is ignored.
     *
     * @param scanPackage base package to scan (e.g., "com.example.listeners")
     * @return this builder
     * @throws IllegalArgumentException if package is null or empty when Spring is configured
     */
    public EventListenerRegistryBuilder scanPackage(String scanPackage) {
        if (springContext != null && (scanPackage == null || scanPackage.isEmpty())) {
            throw new IllegalArgumentException(
                    "Scan package is REQUIRED when using Spring context. " +
                            "Please provide a valid package name (e.g., 'com.example.listeners')"
            );
        }
        this.scanPackage = scanPackage != null ? scanPackage : "";
        return this;
    }

    public EventListenerRegistryBuilder includeAnnotationListeners() {
        this.includeAnnotationListeners = true;
        return this;
    }

    public EventListenerRegistryBuilder includeAnnotationListeners(boolean include) {
        this.includeAnnotationListeners = include;
        return this;
    }

    public EventListenerRegistryBuilder includeInterfaceListeners() {
        this.includeInterfaceListeners = true;
        return this;
    }

    public EventListenerRegistryBuilder includeInterfaceListeners(boolean include) {
        this.includeInterfaceListeners = include;
        return this;
    }

    public EventListenerRegistryBuilder withRegistry(EventListenerRegistry registry) {
        if (registry != null) {
            this.additionalRegistries.add(registry);
        }
        return this;
    }

    public EventListenerRegistryBuilder withDecorator(DecoratorFunction decorator) {
        if (decorator != null) {
            this.decorators.add(decorator);
        }
        return this;
    }

    // ==================== Build methods ====================

    public EventListenerRegistry build() {
        validateConfiguration();

        EventListenerRegistry registry = createBaseRegistry();

        for (DecoratorFunction decorator : decorators) {
            registry = decorator.apply(registry);
        }

        return registry;
    }

    public EventListenerRegistry buildAndLog() {
        EventListenerRegistry registry = build();

        log.info("Built EventListenerRegistry: type={}, springContext={}, scanPackage='{}', " +
                        "includeAnnotation={}, includeInterface={}, customRegistries={}, decorators={}",
                registryType,
                springContext != null ? "yes" : "no",
                springContext != null ? scanPackage : "N/A (no Spring)",
                includeAnnotationListeners,
                includeInterfaceListeners,
                additionalRegistries.size(),
                decorators.size()
        );

        return registry;
    }

    // ==================== Private validation ====================

    private void validateConfiguration() {
        // Rule 1: If Spring context is provided, scanPackage must be specified
        if (springContext != null && scanPackage.isEmpty()) {
            throw new IllegalStateException(
                    "Scan package is REQUIRED when using Spring context.\n" +
                            "Please add .scanPackage(\"com.your.base.package\") to the builder chain.\n" +
                            "Example: EventListenerRegistryBuilder.spring()\n" +
                            "    .scanPackage(\"com.example.listeners\")\n" +
                            "    .withSpringContext(applicationContext)\n" +
                            "    .build();"
            );
        }

        // Rule 2: Spring annotation registry requires scanPackage (redundant but explicit)
        if (registryType == RegistryType.SPRING_ANNOTATION && scanPackage.isEmpty()) {
            throw new IllegalStateException(
                    "Spring annotation registry requires a scan package. " +
                            "Use .scanPackage(\"com.example.package\") to specify it."
            );
        }

        // Rule 3: Spring context required for Spring-based registries
        if (isSpringBased() && springContext == null) {
            throw new IllegalStateException(
                    "Spring context is REQUIRED for Spring-based registries. " +
                            "Use .withSpringContext(applicationContext) to provide it."
            );
        }

        // Rule 4: Composite must have at least one registry
        if (registryType == RegistryType.COMPOSITE &&
                !includeAnnotationListeners &&
                !includeInterfaceListeners &&
                additionalRegistries.isEmpty()) {
            throw new IllegalStateException(
                    "Composite registry must contain at least one registry. " +
                            "Use includeAnnotationListeners(), includeInterfaceListeners(), or withRegistry()."
            );
        }
    }

    private boolean isSpringBased() {
        return registryType == RegistryType.SPRING_ANNOTATION ||
                registryType == RegistryType.SPRING_INTERFACE ||
                (registryType == RegistryType.COMPOSITE && springContext != null);
    }

    // ==================== Registry creation ====================

    private EventListenerRegistry createBaseRegistry() {
        return switch (registryType) {
            case ANNOTATION -> createAnnotationRegistry();
            case INTERFACE -> createInterfaceRegistry();
            case SPRING_ANNOTATION -> createSpringAnnotationRegistry();
            case SPRING_INTERFACE -> createSpringInterfaceRegistry();
            case COMPOSITE -> createCompositeRegistry();
        };
    }

    private EventListenerRegistry createAnnotationRegistry() {
        if (springContext != null) {
            validateSpringAnnotationConfig();
            return createSpringAnnotationRegistry();
        }
        return new AnnotationEventListenerRegistry();
    }

    private EventListenerRegistry createInterfaceRegistry() {
        if (springContext != null) {
            return createSpringInterfaceRegistry();
        }
        return new InterfaceEventListenerRegistry();
    }

    private SpringAnnotationEventListenerRegistry createSpringAnnotationRegistry() {
        validateSpringAnnotationConfig();
        SpringAnnotationEventListenerRegistry registry =
                new SpringAnnotationEventListenerRegistry(springContext, scanPackage);
        registerInSpring(registry, "springAnnotationEventListenerRegistry");
        return registry;
    }

    private SpringInterfaceEventListenerRegistry createSpringInterfaceRegistry() {
        SpringInterfaceEventListenerRegistry registry =
                new SpringInterfaceEventListenerRegistry(springContext);
        registerInSpring(registry, "springInterfaceEventListenerRegistry");
        return registry;
    }

    private void validateSpringAnnotationConfig() {
        if (scanPackage.isEmpty()) {
            throw new IllegalStateException(
                    "Scan package is REQUIRED for Spring annotation registry. " +
                            "Please specify it with .scanPackage(\"com.example.package\")"
            );
        }
    }

    private EventListenerRegistry createCompositeRegistry() {
        List<EventListenerRegistry> registries = new ArrayList<>();
        if (includeAnnotationListeners) {
            registries.add(createAnnotationRegistry());
        }
        if (includeInterfaceListeners) {
            registries.add(createInterfaceRegistry());
        }
        registries.addAll(additionalRegistries);
        return new CompositeEventListenerRegistry(registries);
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

    private enum RegistryType {
        ANNOTATION,
        INTERFACE,
        SPRING_ANNOTATION,
        SPRING_INTERFACE,
        COMPOSITE
    }

    @FunctionalInterface
    public interface DecoratorFunction {
        EventListenerRegistry apply(EventListenerRegistry registry);
    }
}