package com.github.vovten.eventflow.autoconfig;

import com.github.vovten.eventflow.autoconfig.transport.IncomingTransportFactory;
import com.github.vovten.eventflow.dispatcher.EventDispatcher;
import com.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.transport.InMemoryTransportsBuilder.InMemoryTransports;
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
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EventFlowDispatcherConfiguration {

    private final EventFlowProperties properties;
    private final Map<String, IncomingTransportFactory> incomingTransportFactories;

    public EventFlowDispatcherConfiguration(
            EventFlowProperties properties,
            List<IncomingTransportFactory> incomingTransportFactories) {
        this.properties = properties;
        this.incomingTransportFactories = collect(incomingTransportFactories);

        log.info("Registered incoming transport factories: {}", this.incomingTransportFactories.keySet());
    }

    /**
     * Creates event dispatcher with configured transports.
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    public EventDispatcher eventDispatcher(
            @Qualifier("eventListenerRegistry") EventListenerRegistry listenerRegistry,
            ExecutorService dispatcherExecutor,
            List<IncomingEventTransport> incomingEventTransports) {

        log.info("Configuring EventDispatcher");
        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                dispatcherExecutor,
                listenerRegistry,
                incomingEventTransports
        );
        dispatcher.start();
        return dispatcher;
    }

    /**
     * Creates incoming transports from configuration (excludes in-memory, handled separately).
     */
    @Bean
    @ConditionalOnMissingBean
    public List<IncomingEventTransport> incomingEventTransports(InMemoryTransports inMemoryTransports) {
        List<IncomingEventTransport> transports = properties.getDispatcher().getTransports().stream()
                .filter(config -> !"in-memory".equalsIgnoreCase(config.getType()))
                .map(this::createIncomingTransport)
                .peek(transport -> log.info("Created incoming transport: {} ({})",
                        transport.name(), transport.getClass().getSimpleName()))
                .toList();
        List<IncomingEventTransport> allTransports = new ArrayList<>();
        allTransports.add(inMemoryTransports.incoming());
        allTransports.addAll(transports);
        return allTransports;
    }

    private IncomingEventTransport createIncomingTransport(EventFlowProperties.TransportConfig config) {
        IncomingTransportFactory factory = incomingTransportFactories.get(config.getType());
        if (factory == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported transport type '%s' for incoming transport. Supported types: %s",
                            config.getType(),
                            incomingTransportFactories.keySet()
                    )
            );
        }
        factory.validate(config);
        return factory.createIncoming(config);
    }

    private Map<String, IncomingTransportFactory> collect(List<IncomingTransportFactory> incomingTransportFactories) {
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
