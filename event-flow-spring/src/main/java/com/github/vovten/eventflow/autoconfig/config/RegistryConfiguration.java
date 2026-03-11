package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.registry.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringAnnotationEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceEventListenerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * Auto-configuration for event listener registries.
 * <p>
 * Configures:
 * <ul>
 *   <li>{@link SpringAnnotationEventListenerRegistry} - for @EventListener annotated beans</li>
 *   <li>{@link SpringInterfaceEventListenerRegistry} - for interface-based listeners</li>
 *   <li>{@link CompositeEventListenerRegistry} - combines all registries</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RegistryConfiguration {

    private final EventFlowProperties properties;

    public RegistryConfiguration(EventFlowProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates Spring-aware annotation-based listener registry.
     */
    @Bean
    @ConditionalOnMissingBean(name = "springAnnotationEventListenerRegistry")
    public EventListenerRegistry springAnnotationEventListenerRegistry(ApplicationContext appContext) {
        String scanPackage = properties.getScanPackages();
        if (scanPackage == null || scanPackage.isEmpty()) {
            throw new IllegalStateException("event-flow.scan-packages must be configured");
        }
        log.info("Creating SpringAnnotationEventListenerRegistry with scan package: {}", scanPackage);
        return new SpringAnnotationEventListenerRegistry(appContext, scanPackage);
    }

    /**
     * Creates Spring-aware interface-based listener registry.
     */
    @Bean
    @ConditionalOnMissingBean(name = "springInterfaceEventListenerRegistry")
    public EventListenerRegistry springInterfaceEventListenerRegistry(ApplicationContext appContext) {
        log.info("Creating SpringInterfaceEventListenerRegistry");
        return new SpringInterfaceEventListenerRegistry(appContext);
    }

    /**
     * Creates composite listener registry from all available registries.
     */
    @Bean
    @ConditionalOnMissingBean
    public EventListenerRegistry eventListenerRegistry(List<EventListenerRegistry> registries) {
        String registryNames = registries.stream()
            .map(EventListenerRegistry::name)
            .collect(joining(", "));
        log.info("Creating CompositeEventListenerRegistry with {} registries: {}",
            registries.size(), registryNames);
        return new CompositeEventListenerRegistry(registries);
    }
}
