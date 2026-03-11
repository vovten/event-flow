package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.OutgoingTransportFactory;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.transport.InMemoryTransportsBuilder;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

/**
 * Auto-configuration for event channels.
 * <p>
 * Creates internal and external channels based on configuration.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ChannelConfiguration {

    private final EventFlowProperties properties;
    private final Map<String, OutgoingTransportFactory> outgoingTransportFactories;

    public ChannelConfiguration(
            EventFlowProperties properties,
            List<OutgoingTransportFactory> outgoingTransportFactories) {
        this.properties = properties;
        this.outgoingTransportFactories = collect(outgoingTransportFactories);
        log.info("Registered outgoing transport factories: {}", this.outgoingTransportFactories.keySet());
    }

    /**
     * Creates all event channels
     */
    @Bean
    @ConditionalOnMissingBean(name = "eventChannels")
    public List<EventChannel> eventChannels(InMemoryTransportsBuilder.InMemoryTransports inMemoryTransports) {
        List<EventChannel> channels = new ArrayList<>();
        channels.add(createInternalChannel(inMemoryTransports));
        channels.addAll(createConfiguredChannels());
        log.info("Created {} event channels: {}",
            channels.size(),
            channels.stream().map(EventChannel::name).collect(joining(", ")));
        return channels;
    }

    /**
     * Create configured channels from properties (excluding internal)
     */
    private List<EventChannel> createConfiguredChannels() {
        return properties.getPublisher().getChannels().stream()
            .filter(config -> !"internal".equalsIgnoreCase(config.getName()))
            .map(this::createEventChannel)
            .toList();
    }

    /**
     * Creates internal channel.
     * <p>
     * If internal channel is explicitly configured in properties, uses that configuration.
     * Otherwise, defaults to in-memory transport with shared queue.
     */
    private EventChannel createInternalChannel(InMemoryTransportsBuilder.InMemoryTransports inMemoryTransports) {
        // Check if internal channel is explicitly configured
        var internalConfig = properties.getPublisher().getChannels().stream()
            .filter(config -> "internal".equalsIgnoreCase(config.getName()))
            .findFirst();

        if (internalConfig.isPresent()) {
            log.info("Creating internal channel with configured transport: {}", internalConfig.get().getType());
            List<OutgoingEventTransport> transports = createOutgoingTransports(internalConfig.get());
            return new InternalEventChannel(transports);
        }

        // Default: use in-memory transport with shared queue
        log.debug("Creating internal event channel with default in-memory transport");
        return new InternalEventChannel(List.of(inMemoryTransports.outgoing()));
    }

    /**
     * Create event channel from configuration.
     */
    private EventChannel createEventChannel(EventFlowProperties.ChannelConfig config) {
        List<OutgoingEventTransport> transports = createOutgoingTransports(config);
        return switch (config.getName().toLowerCase()) {
            case "internal" -> new InternalEventChannel(transports);
            case "external" -> new ExternalEventChannel(transports);
            default -> new GenericEventChannel(config.getName(), transports);
        };
    }

    /**
     * Create outgoing transports for channel configuration.
     * Delegates to appropriate transport factory.
     */
    private List<OutgoingEventTransport> createOutgoingTransports(EventFlowProperties.ChannelConfig config) {
        OutgoingTransportFactory factory = outgoingTransportFactories.get(config.getType());
        if (factory == null) {
            throw new IllegalArgumentException(
                String.format("Unsupported transport type '%s' for channel '%s'. " +
                    "Supported types: %s",
                    config.getType(),
                    config.getName(),
                    outgoingTransportFactories.keySet())
            );
        }
        factory.validate(config);
        return List.of(factory.createOutgoing(config));
    }

    /**
     * Collect unique map of {@linkplain #outgoingTransportFactories}
     */
    private static Map<String, OutgoingTransportFactory> collect(List<OutgoingTransportFactory> outgoingTransportFactories) {
        return outgoingTransportFactories.stream()
                .collect(java.util.stream.Collectors.toMap(
                        OutgoingTransportFactory::getType,
                        Function.identity(),
                        (existing, replacement) -> {
                            log.warn("Duplicate outgoing transport factory for type '{}', using: {}",
                                    existing.getType(), existing.getClass().getSimpleName());
                            return existing;
                        }
                ));
    }

    /**
     * Generic event channel for custom channel names.
     */
    private record GenericEventChannel(String name, List<OutgoingEventTransport> transports) implements EventChannel {
    }
}
