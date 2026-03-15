package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.IncomingTransportFactory;
import com.github.vovten.eventflow.dispatcher.EventDispatcher;
import com.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import com.github.vovten.eventflow.registry.EventHandlerRegistry;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
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
 * Creates incoming transports using configured factories.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DispatcherConfiguration {

    private final EventFlowProperties properties;
    private final Map<String, IncomingTransportFactory> incomingTransportFactories;

    public DispatcherConfiguration(EventFlowProperties properties,
                                   List<IncomingTransportFactory> incomingTransportFactories) {
        this.properties = properties;
        this.incomingTransportFactories = collect(incomingTransportFactories);
        log.info("Registered incoming transport factories: {}", this.incomingTransportFactories.keySet());
    }

    /**
     * Creates event dispatcher with configured transports.
     * Only created when event-flow is enabled.
     *
     * @param dispatcherExecutor      executor service for dispatcher
     * @param eventHandlerRegistry    event handler registry
     * @param incomingEventTransports list of incoming event transports
     * @return event dispatcher instance
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
    public EventDispatcher eventDispatcher(@Qualifier("dispatcherExecutor") ExecutorService dispatcherExecutor,
                                           @Qualifier("eventHandlerRegistry") EventHandlerRegistry eventHandlerRegistry,
                                           @Qualifier("incomingEventTransports") List<IncomingEventTransport> incomingEventTransports) {
        log.info("Configuring EventDispatcher with {} transports", incomingEventTransports.size());
        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                dispatcherExecutor,
                eventHandlerRegistry,
                incomingEventTransports
        );
        dispatcher.start();
        return dispatcher;
    }

    /**
     * Creates incoming transports from configuration using factories.
     * Transport name identifies the type (e.g., "in-memory", "kafka").
     * Only created when event-flow is enabled.
     *
     * @return list of incoming event transports
     */
    @Bean("incomingEventTransports")
    @ConditionalOnMissingBean(name = "incomingEventTransports")
    @ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
    public List<IncomingEventTransport> incomingEventTransports() {
        List<IncomingEventTransport> transports = new ArrayList<>();
        // Create transports from configuration
        if (!properties.getDispatcher().getTransports().isEmpty()) {
            for (EventFlowProperties.TransportConfig config : properties.getDispatcher().getTransports()) {
                IncomingTransportFactory factory = incomingTransportFactories.get(config.getName());
                if (factory == null) {
                    throwUnsupportedTransportException(config);
                }
                factory.validate(config);
                IncomingEventTransport transport = factory.createIncoming(config);
                transports.add(transport);
                log.info("Created incoming transport '{}' ({})", config.getName(), config.getName());
            }
        } else {
            // Default: create in-memory transport if factory available
            IncomingTransportFactory factory = incomingTransportFactories.get("in-memory");
            if (factory != null) {
                EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
                config.setName("in-memory");
                config.setCapacity(1000);
                IncomingEventTransport transport = factory.createIncoming(config);
                transports.add(transport);
                log.info("Created default in-memory incoming transport");
            }
        }
        return transports;
    }

    private void throwUnsupportedTransportException(EventFlowProperties.TransportConfig config) {
        String msg = "Unsupported transport type '%s'. Supported types: %s";
        throw new IllegalArgumentException(String.format(msg, config.getName(), incomingTransportFactories.keySet()));
    }

    /**
     * Collect unique map of incoming transport factories.
     *
     * @param incomingTransportFactories list of factories
     * @return map of factories by type
     */
    private static Map<String, IncomingTransportFactory> collect(List<IncomingTransportFactory> incomingTransportFactories) {
        return incomingTransportFactories.stream()
                .collect(toMap(
                        IncomingTransportFactory::getType,
                        Function.identity(),
                        (existing, replacement) -> {
                            log.warn("Duplicate incoming transport factory for type '{}', using: {}",
                                    existing.getType(), existing.getClass().getSimpleName());
                            return existing;
                        }
                ));
    }
}
