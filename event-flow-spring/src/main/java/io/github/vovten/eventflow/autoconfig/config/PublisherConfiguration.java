package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.lifecycle.EventLifecyclePublisher;
import io.github.vovten.eventflow.publisher.CircuitBreakerEventPublisher;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.FailureTracker;
import io.github.vovten.eventflow.publisher.SpringEventPublisherBuilder;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * Auto-configuration for event publisher.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true")
public class PublisherConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PublisherConfiguration.class);

    private final EventFlowProperties properties;
    private final Map<String, OutTransportFactory> publisherTransportFactories;
    private CircuitBreakerEventPublisher circuitBreaker;

    public PublisherConfiguration(EventFlowProperties properties,
                                  List<OutTransportFactory> publisherTransportFactories) {
        this.properties = properties;
        this.publisherTransportFactories = collect(publisherTransportFactories);
        log.info("Registered publisher transport factories: {}", this.publisherTransportFactories.keySet());
    }

    /**
     * Creates event publisher with all configured channels.
     *
     * @param eventChannels list of event channels to configure
     * @param eventStore    optional event store for persistent publishing (auto-injected)
     * @return configured event publisher
     * @throws IllegalStateException if lifecycle is enabled but service-name is not configured
     */
    @Bean
    @ConditionalOnMissingBean(name = "eventPublisher")
    @ConditionalOnProperty(prefix = "event-flow.publisher", name = "enabled", havingValue = "true")
    public EventPublisher eventPublisher(List<EventChannel> eventChannels,
                                         @Autowired(required = false) EventStore eventStore) {
        if (eventChannels.isEmpty()) {
            log.warn("""

                   ╔═════════════════════════════════════════════════════════════╗
                   ║ Event Flow Publisher enabled but no channels configured     ║
                   ║ To enable event publishing, add at least one channel:       ║
                   ╚═════════════════════════════════════════════════════════════╝
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
            return null;
        }
        logInfo(eventChannels);
        EventFlowProperties.PublisherConfig publisherConfig = properties.getPublisher();

        // Use Spring-aware builder
        SpringEventPublisherBuilder builder = SpringEventPublisherBuilder.create(eventChannels);

        // Apply retry if enabled
        var retry = publisherConfig.getRetry();
        if (retry.isEnabled()) {
            builder.retryable(retry.getMaxAttempts(), retry.getInitialDelay(), retry.getMultiplier());
        }

        // Apply transactional if enabled
        if (publisherConfig.isTransactional()) {
            builder.transactional();
        }

        // Apply logging if enabled
        var loggingConfig = publisherConfig.getLogging();
        if (loggingConfig.isEnabled()) {
            builder.loggable(loggingConfig.getMaxPayloadLength(),
                    Set.copyOf(loggingConfig.getExcludedEvents()),
                    loggingConfig.getLogLevels());
        }

        EventPublisher publisher = builder.build();

        // Apply circuit breaker if enabled (wraps transport layer, before lifecycle)
        var cbConfig = publisherConfig.getCircuitBreaker();
        if (cbConfig.isEnabled()) {
            circuitBreaker = new CircuitBreakerEventPublisher(
                    publisher,
                    cbConfig.getFailureThreshold(),
                    cbConfig.getFailureRateThreshold(),
                    cbConfig.getCooldown(),
                    cbConfig.getHalfOpenMaxAttempts(),
                    cbConfig.getMaxCacheSize()
            );
            publisher = circuitBreaker;
            log.info("Wrapped EventPublisher with CircuitBreakerEventPublisher (maxCacheSize={})",
                    cbConfig.getMaxCacheSize());
        }

        // Wrap with lifecycle-aware publisher if enabled
        if (eventStore != null && publisherConfig.getLifecycle().isEnabled()) {
            String service = publisherConfig.getLifecycle().getServiceName();
            if (StringUtils.isEmpty(service)) {
                throw new IllegalStateException(
                        "event-flow.publisher.lifecycle.service-name must be configured when lifecycle tracking is enabled");
            }
            publisher = new EventLifecyclePublisher(publisher, eventStore, service);
            log.info("Wrapped EventPublisher with EventLifecyclePublisher (service: {})", service);
        }

        log.info("Built EventPublisher with configuration: channels={}, retry={}, circuitBreaker={}, lifecycle={}, customDecorators={}",
                eventChannels.size(),
                retry.isEnabled() ? "enabled" : "disabled",
                cbConfig.isEnabled() ? "enabled" : "disabled",
                publisherConfig.getLifecycle().isEnabled() ? "enabled" : "disabled",
                "0"
        );
        return publisher;
    }

    /**
     * Exposes the circuit breaker as a {@link FailureTracker} bean for lifecycle
     * integration. When present, {@link AckHandler} uses this to record handler-side
     * failures into the circuit breaker.
     *
     * @return the circuit breaker as a FailureTracker, or null if circuit breaker is disabled
     */
    @Bean
    @ConditionalOnProperty(prefix = "event-flow.publisher.circuit-breaker", name = "enabled", havingValue = "true")
    public FailureTracker circuitBreakerFailureTracker() {
        return circuitBreaker;
    }

    private static void logInfo(List<EventChannel> eventChannels) {
        String msg = "Configuring EventPublisher with {} channels: {}";
        String channelNames = eventChannels.stream().map(EventChannel::name).collect(joining(", "));
        log.info(msg, eventChannels.size(), channelNames);
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
}
