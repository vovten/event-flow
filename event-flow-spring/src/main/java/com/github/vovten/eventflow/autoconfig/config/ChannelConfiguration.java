package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.OutgoingTransportFactory;
import com.github.vovten.eventflow.channel.BroadcastEventChannel;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
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
import static java.util.stream.Collectors.toMap;

/**
 * Auto-configuration for event channels.
 * <p>
 * Creates internal and external channels based on configuration.
 * Creates outgoing transports for each channel from transport configurations.
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

    public ChannelConfiguration(EventFlowProperties properties,
                                List<OutgoingTransportFactory> outgoingTransportFactories) {
        this.properties = properties;
        this.outgoingTransportFactories = collect(outgoingTransportFactories);
        log.info("Registered outgoing transport factories: {}", this.outgoingTransportFactories.keySet());
    }

    /**
     * Creates all event channels.
     *
     * @return list of created event channels
     */
    @Bean
    @ConditionalOnMissingBean(name = "eventChannels")
    public List<EventChannel> eventChannels() {
        List<EventChannel> channels = new ArrayList<>(createConfiguredChannels());
        log.info("Created {} event channels: {}",
                channels.size(),
                channels.stream().map(EventChannel::name).collect(joining(", "))
        );
        return channels;
    }

    /**
     * Create configured channels from properties.
     *
     * @return list of configured event channels
     */
    private List<EventChannel> createConfiguredChannels() {
        return properties.getPublisher().getChannels().stream()
            .map(this::createEventChannel)
            .toList();
    }

    /**
     * Create event channel from configuration.
     *
     * @param config channel configuration
     * @return created event channel
     */
    private EventChannel createEventChannel(EventFlowProperties.ChannelConfig config) {
        List<OutgoingEventTransport> transports = createOutgoingTransports(config);
        return switch (config.getName().toLowerCase()) {
            case "internal" -> new InternalEventChannel(transports);
            case "external" -> new ExternalEventChannel(transports);
            case "broadcast" -> new BroadcastEventChannel(transports);
            default -> new GenericEventChannel(config.getName(), transports);
        };
    }

    /**
     * Create outgoing transports for channel configuration.
     * Uses transport name to select factory.
     *
     * @param config channel configuration
     * @return list of outgoing event transports
     * @throws IllegalArgumentException if channel has no transports configured
     */
    private List<OutgoingEventTransport> createOutgoingTransports(EventFlowProperties.ChannelConfig config) {
        if (config.getTransports().isEmpty()) {
            String msg = "Channel '%s' must have at least one transport configured";
            throw new IllegalArgumentException(String.format(msg, config.getName()));
        }
        return config.getTransports().stream()
            .map(this::createOutgoingTransport)
            .toList();
    }

    private OutgoingEventTransport createOutgoingTransport(EventFlowProperties.TransportConfig transportConfig) {
        String name = transportConfig.getName();
        OutgoingTransportFactory factory = outgoingTransportFactories.get(name);
        if (factory == null) {
            String msg = "No factory found for transport name '%s'. Available factories: %s";
            throw new IllegalStateException(String.format(msg, name, outgoingTransportFactories.keySet()));
        }
        factory.validate(transportConfig);
        return factory.createOutgoing(transportConfig);
    }

    /**
     * Collect unique map of outgoing transport factories.
     *
     * @param outgoingTransportFactories list of factories
     * @return map of factories by type
     */
    private static Map<String, OutgoingTransportFactory> collect(List<OutgoingTransportFactory> outgoingTransportFactories) {
        return outgoingTransportFactories.stream()
                .collect(toMap(
                        OutgoingTransportFactory::getName,
                        Function.identity(),
                        (existing, replacement) -> {
                            log.warn("Duplicate outgoing transport factory for type '{}', using: {}",
                                    existing.getName(), existing.getClass().getSimpleName());
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
