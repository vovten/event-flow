package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.DispatcherTransportFactory;
import com.github.vovten.eventflow.dispatcher.EventDispatcher;
import com.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import com.github.vovten.eventflow.registry.EventHandlerRegistry;
import com.github.vovten.eventflow.transport.DispatcherTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Auto-configuration for event dispatcher.
 * <p>
 * Creates dispatcher transports using configured factories.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DispatcherConfiguration {

    private final EventFlowProperties properties;
    private final Map<String, DispatcherTransportFactory> dispatcherTransportFactories;

    public DispatcherConfiguration(EventFlowProperties properties,
                                   List<DispatcherTransportFactory> dispatcherTransportFactories) {
        this.properties = properties;
        this.dispatcherTransportFactories = collect(dispatcherTransportFactories);
        log.info("Registered dispatcher transport factories: {}", this.dispatcherTransportFactories.keySet());
    }

    /**
     * Creates event dispatcher with configured transports.
     * Only created when event-flow is enabled.
     *
     * @param dispatcherExecutor      executor service for dispatcher
     * @param eventHandlerRegistry    event handler registry
     * @param dispatcherTransports    list of dispatcher transports
     * @return event dispatcher instance
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
    public EventDispatcher eventDispatcher(@Qualifier("dispatcherExecutor") ExecutorService dispatcherExecutor,
                                           @Qualifier("eventHandlerRegistry") EventHandlerRegistry eventHandlerRegistry,
                                           @Qualifier("dispatcherTransports") List<DispatcherTransport> dispatcherTransports) {
        log.info("Configuring EventDispatcher with {} transports", dispatcherTransports.size());
        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                dispatcherExecutor,
                eventHandlerRegistry,
                dispatcherTransports
        );
        dispatcher.start();
        return dispatcher;
    }

    /**
     * Creates dispatcher transports from configuration using factories.
     * Transport name identifies the type (e.g., "in-memory", "kafka").
     * Only created when event-flow is enabled.
     *
     * @return list of dispatcher transports
     */
    @Bean("dispatcherTransports")
    @ConditionalOnMissingBean(name = "dispatcherTransports")
    @ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
    public List<DispatcherTransport> dispatcherTransports() {
        List<DispatcherTransport> transports = new ArrayList<>();
        // Create transports from configuration
        if (!properties.getDispatcher().getTransports().isEmpty()) {
            for (EventFlowProperties.TransportConfig config : properties.getDispatcher().getTransports()) {
                DispatcherTransportFactory factory = dispatcherTransportFactories.get(config.getName());
                if (factory == null) {
                    throwUnsupportedTransportException(config);
                }
                factory.validate(config);
                DispatcherTransport transport = factory.createDispatcher(config);
                transports.add(transport);
                log.info("Created dispatcher transport '{}' ({})", config.getName(), config.getName());
            }
        } else {
            // Default: create in-memory transport if factory available
            DispatcherTransportFactory factory = dispatcherTransportFactories.get("in-memory");
            if (factory != null) {
                EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
                config.setName("in-memory");
                config.setCapacity(1000);
                DispatcherTransport transport = factory.createDispatcher(config);
                transports.add(transport);
                log.info("Created default in-memory dispatcher transport");
            }
        }
        return transports;
    }

    private void throwUnsupportedTransportException(EventFlowProperties.TransportConfig config) {
        String msg = "Unsupported transport type '%s'. Supported types: %s";
        throw new IllegalArgumentException(String.format(msg, config.getName(), dispatcherTransportFactories.keySet()));
    }

    /**
     * Collect unique map of dispatcher transport factories.
     *
     * @param dispatcherTransportFactories list of factories
     * @return map of factories by type
     */
    private static Map<String, DispatcherTransportFactory> collect(List<DispatcherTransportFactory> dispatcherTransportFactories) {
        return dispatcherTransportFactories.stream()
                .collect(toMap(
                        DispatcherTransportFactory::getType,
                        Function.identity(),
                        (existing, replacement) -> {
                            log.warn("Duplicate dispatcher transport factory for type '{}', using: {}",
                                    existing.getType(), existing.getClass().getSimpleName());
                            return existing;
                        }
                ));
    }
}
