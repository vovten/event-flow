package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.registry.CompositeEventHandlerRegistry;
import com.github.vovten.eventflow.registry.EventHandlerRegistry;
import com.github.vovten.eventflow.registry.SpringEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringEventSubscriberRegistry;
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
 *   <li>{@link SpringEventListenerRegistry} - for @EventListener annotated beans</li>
 *   <li>{@link SpringEventSubscriberRegistry} - for interface-based subscribers</li>
 *   <li>{@link CompositeEventHandlerRegistry} - combines all registries</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true")
public class RegistryConfiguration {

    private final EventFlowProperties properties;

    public RegistryConfiguration(EventFlowProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates Spring-aware annotation-based listener registry.
     *
     * @param appContext Spring application context
     * @return annotation-based event listener registry
     * @throws IllegalStateException if listener-packages is not configured
     */
    @Bean
    @ConditionalOnMissingBean(name = "springEventListenerRegistry")
    public EventHandlerRegistry springEventListenerRegistry(ApplicationContext appContext) {
        String listenerPackage = properties.getDispatcher().getListenerPackages();
        if (listenerPackage == null || listenerPackage.isEmpty()) {
            throw new IllegalStateException("event-flow.dispatcher.listener-packages must be configured");
        }
        log.info("Creating SpringEventListenerRegistry with listener package: {}", listenerPackage);
        return new SpringEventListenerRegistry(appContext, listenerPackage);
    }

    /**
     * Creates Spring-aware interface-based subscriber registry.
     *
     * @param appContext Spring application context
     * @return interface-based event subscriber registry
     */
    @Bean
    @ConditionalOnMissingBean(name = "springEventSubscriberRegistry")
    public EventHandlerRegistry springEventSubscriberRegistry(ApplicationContext appContext) {
        log.info("Creating SpringEventSubscriberRegistry");
        return new SpringEventSubscriberRegistry(appContext);
    }

    /**
     * Creates composite listener registry from all available registries.
     *
     * @param registries list of event listener registries to combine
     * @return composite event listener registry
     */
    @Bean("eventHandlerRegistry")
    public EventHandlerRegistry eventHandlerRegistry(List<EventHandlerRegistry> registries) {
        String registryNames = registries.stream()
                .map(EventHandlerRegistry::name)
                .collect(joining(", "));
        log.info("Creating CompositeEventHandlerRegistry with {} registries: {}",
                registries.size(), registryNames);
        return new CompositeEventHandlerRegistry(registries);
    }
}
