package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.InTransportFactory;
import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.dispatcher.EventDispatcherBuilder;
import io.github.vovten.eventflow.lifecycle.EventLifecycleDispatcher;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.transport.InTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Auto-configuration for event dispatcher.
 * <p>
 * Creates dispatcher transports using configured factories.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true")
public class DispatcherConfiguration {

    private static final Logger log = LoggerFactory.getLogger(DispatcherConfiguration.class);

    private final EventFlowProperties properties;
    private final Map<String, InTransportFactory> dispatcherTransportFactories;

    public DispatcherConfiguration(EventFlowProperties properties,
                                   List<InTransportFactory> dispatcherTransportFactories) {
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
     * @param inTransports            list of dispatcher transports
     * @param ackPublisher            optional publisher for lifecycle ack events
     * @return event dispatcher instance
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.dispatcher", name = "enabled", havingValue = "true")
    public EventDispatcher eventDispatcher(@Qualifier("dispatcherExecutor") ExecutorService dispatcherExecutor,
                                           @Qualifier("eventHandlerRegistry") EventHandlerRegistry eventHandlerRegistry,
                                           @Qualifier("dispatcherTransports") List<InTransport> inTransports,
                                           @Autowired(required = false) EventPublisher ackPublisher) {
        EventDispatcherBuilder builder = EventDispatcherBuilder.create()
                .executor(dispatcherExecutor)
                .handlerRegistry(eventHandlerRegistry)
                .transports(inTransports);
        int concurrencyLimit = properties.getDispatcher().getThreadPool().getConcurrencyLimit();
        if (concurrencyLimit > 0) {
            builder.concurrencyLimit(concurrencyLimit);
        }
        if (properties.getDispatcher().getIdempotent().isEnabled()) {
            EventFlowProperties.IdempotentConfig config = properties.getDispatcher().getIdempotent();
            builder.idempotent(config.getTtl(), config.getMaxSize(), config.isWarnOnDuplicate());
        }
        var loggingConfig = properties.getDispatcher().getLogging();
        if (loggingConfig.isEnabled()) {
            builder.loggable(loggingConfig.getMaxPayloadLength(),
                    Set.copyOf(loggingConfig.getExcludedEvents()),
                    loggingConfig.getLogLevels());
        }
        EventDispatcher dispatcher = builder.build();

        // Wrap with lifecycle tracking if enabled and ack publisher is available
        if (ackPublisher != null && properties.getDispatcher().getLifecycle().isEnabled()) {
            dispatcher = new EventLifecycleDispatcher(dispatcher, ackPublisher);
            log.info("Wrapped EventDispatcher with EventLifecycleDispatcher (ack publisher available)");
        } else if (properties.getDispatcher().getLifecycle().isEnabled()) {
            log.warn("Lifecycle tracking enabled but no EventPublisher available for ack events");
        }

        log.info("Built EventDispatcher with configuration: transports={}, idempotent={}, logging={}, lifecycle={}",
                inTransports.size(),
                properties.getDispatcher().getIdempotent().isEnabled() ? "enabled" : "disabled",
                properties.getDispatcher().getLogging().isEnabled() ? "enabled" : "disabled",
                properties.getDispatcher().getLifecycle().isEnabled() ? "enabled" : "disabled"
        );
        dispatcher.start(dispatcher::dispatch);
        return dispatcher;
    }

    /**
     * Creates dispatcher transports from configuration using factories.
     * Transport name identifies the type (e.g., "local-queue", "kafka").
     * At least one transport must be explicitly configured when dispatcher is enabled.
     * Only created when event-flow is enabled.
     *
     * @return list of dispatcher transports
     */
    @Bean("dispatcherTransports")
    @ConditionalOnMissingBean(name = "dispatcherTransports")
    @ConditionalOnProperty(prefix = "event-flow.dispatcher", name = "enabled", havingValue = "true")
    public List<InTransport> dispatcherTransports() {
        List<InTransport> transports = new ArrayList<>();
        // Create transports from configuration
        if (!properties.getDispatcher().getTransports().isEmpty()) {
            for (EventFlowProperties.TransportConfig config : properties.getDispatcher().getTransports()) {
                InTransportFactory factory = dispatcherTransportFactories.get(config.getName());
                if (factory == null) {
                    throwUnsupportedTransportException(config);
                }
                factory.validate(config);
                InTransport transport = factory.createDispatcher(config);
                transports.add(transport);
                log.info("Created dispatcher transport '{}' ({})", config.getName(), config.getName());
            }
        } else {
            log.warn("""
                   
                   ╔═════════════════════════════════════════════════════════════╗
                   ║ Event Flow Dispatcher enabled but no transports configured  ║
                   ║ To enable event dispatching, add at least one transport:    ║
                   ╚═════════════════════════════════════════════════════════════╝
                    event-flow:
                      dispatcher:
                        enabled: true
                        transports:
                          - name: local-queue
                            capacity: 1000
                    Available transport types: {}
                    """, dispatcherTransportFactories.keySet());
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
    private static Map<String, InTransportFactory> collect(List<InTransportFactory> dispatcherTransportFactories) {
        return dispatcherTransportFactories.stream()
                .collect(toMap(
                        InTransportFactory::getType,
                        Function.identity(),
                        (existing, replacement) -> {
                            log.warn("Duplicate dispatcher transport factory for type '{}', using: {}",
                                    existing.getType(), existing.getClass().getSimpleName());
                            return existing;
                        }
                ));
    }
}
