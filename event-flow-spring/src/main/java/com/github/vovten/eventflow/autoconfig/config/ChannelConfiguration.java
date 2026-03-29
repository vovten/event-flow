package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import com.github.vovten.eventflow.channel.BroadcastEventChannel;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.transport.OutTransport;
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
 * Creates publisher transports for each channel from transport configurations.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true")
public class ChannelConfiguration {

    private final EventFlowProperties properties;
    private final Map<String, OutTransportFactory> publisherTransportFactories;

    public ChannelConfiguration(EventFlowProperties properties,
                                List<OutTransportFactory> publisherTransportFactories) {
        this.properties = properties;
        this.publisherTransportFactories = collect(publisherTransportFactories);
        log.info("Registered publisher transport factories: {}", this.publisherTransportFactories.keySet());
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
        if (properties.getPublisher().getChannels().isEmpty()) {
            log.warn("""
                   
                   ╔═══════════════════════════════════════════════════════════╗
                   ║ Event Flow Publisher enabled but no channels configured   ║
                   ║ To enable event publishing, add at least one channel:     ║
                   ╚═══════════════════════════════════════════════════════════╝
                    event-flow:
                      publisher:
                        enabled: true
                        channels:
                          - name: internal
                            transports:
                              - name: local-queue
                                capacity: 1000
                    Available transport types: {}
                    """, publisherTransportFactories.keySet());
        }
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
        List<OutTransport> transports = createPublisherTransports(config);
        return switch (config.getName().toLowerCase()) {
            case "internal" -> new InternalEventChannel(transports);
            case "external" -> new ExternalEventChannel(transports);
            case "broadcast" -> new BroadcastEventChannel(transports);
            default -> new GenericEventChannel(config.getName(), transports);
        };
    }

    /**
     * Create publisher transports for channel configuration.
     * Uses transport name to select factory.
     *
     * @param config channel configuration
     * @return list of publisher transports
     * @throws IllegalArgumentException if channel has no transports configured
     */
    private List<OutTransport> createPublisherTransports(EventFlowProperties.ChannelConfig config) {
        if (config.getTransports().isEmpty()) {
            String msg = "Channel '%s' must have at least one transport configured";
            throw new IllegalArgumentException(String.format(msg, config.getName()));
        }
        return config.getTransports().stream()
            .map(this::createPublisherTransport)
            .toList();
    }

    private OutTransport createPublisherTransport(EventFlowProperties.TransportConfig transportConfig) {
        String name = transportConfig.getName();
        OutTransportFactory factory = publisherTransportFactories.get(name);
        if (factory == null) {
            String msg = "No factory found for transport name '%s'. Available factories: %s";
            throw new IllegalStateException(String.format(msg, name, publisherTransportFactories.keySet()));
        }
        factory.validate(transportConfig);
        return factory.createPublisher(transportConfig);
    }

    /**
     * Collect unique map of publisher transport factories.
     *
     * @param publisherTransportFactories list of factories
     * @return map of factories by type
     */
    private static Map<String, OutTransportFactory> collect(List<OutTransportFactory> publisherTransportFactories) {
        return publisherTransportFactories.stream()
                .collect(toMap(
                        OutTransportFactory::getName,
                        Function.identity(),
                        (existing, replacement) -> {
                            log.warn("Duplicate publisher transport factory for type '{}', using: {}",
                                    existing.getName(), existing.getClass().getSimpleName());
                            return existing;
                        }
                ));
    }

    /**
     * Generic event channel for custom channel names.
     */
    private record GenericEventChannel(String name, List<OutTransport> transports) implements EventChannel {
    }
}
