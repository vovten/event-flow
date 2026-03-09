package com.github.vovten.eventflow.autoconfig;

import com.github.vovten.eventflow.autoconfig.transport.TransportFactory;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.dispatcher.EventDispatcher;
import com.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import com.github.vovten.eventflow.publisher.EventPublisher;
import com.github.vovten.eventflow.publisher.EventPublisherBuilder;
import com.github.vovten.eventflow.publisher.TransactionalEventPublisher;
import com.github.vovten.eventflow.registry.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringAnnotationEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceEventListenerRegistry;
import com.github.vovten.eventflow.transport.InMemoryTransportsBuilder;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

/**
 * Auto-configuration for Event Flow components in Spring applications.
 * <p>
 * Automatically configures:
 * <ul>
 *   <li>{@link EventListenerRegistry} - for managing event listeners</li>
 *   <li>{@link EventChannel} - for routing events</li>
 *   <li>{@link EventPublisher} - for publishing events through channels</li>
 *   <li>{@link EventDispatcher} - for dispatching events to registered listeners</li>
 * </ul>
 * <p>
 * <b>Configuration options:</b>
 * <ul>
 *   <li>Set {@code event-flow.enabled=false} to disable all auto-configuration</li>
 *   <li>Set {@code event-flow.publisher.enabled=false} to disable publisher only</li>
 *   <li>Set {@code event-flow.dispatcher.enabled=false} to disable dispatcher only</li>
 * </ul>
 * <p>
 * <b>Usage example (application.yml):</b>
 * <pre>{@code
 * event-flow:
 *   scan-packages: com.example.listener
 *   publisher:
 *     transactional: true
 *     retry:
 *       enabled: true
 *       max-attempts: 3
 *   dispatcher:
 *     thread-pool:
 *       core-size: 4
 *       max-size: 16
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@Slf4j
@AutoConfiguration
@ConditionalOnClass(EventPublisher.class)
@EnableConfigurationProperties(EventFlowProperties.class)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EventFlowAutoConfiguration {

    private final EventFlowProperties properties;
    private final Map<String, TransportFactory> transportFactories;

    public EventFlowAutoConfiguration(
            EventFlowProperties properties,
            List<TransportFactory> transportFactories) {
        
        this.properties = properties;
        this.transportFactories = transportFactories.stream()
            .collect(Collectors.toMap(
                TransportFactory::getType,
                Function.identity(),
                (existing, replacement) -> {
                    log.warn("Duplicate transport factory for type '{}', using: {}", 
                        existing.getType(), existing.getClass().getSimpleName());
                    return existing;
                }
            ));
        
        log.info("Registered transport factories: {}", this.transportFactories.keySet());
    }

    // ========================================================================
    // Registry Beans
    // ========================================================================

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

    // ========================================================================
    // Executor and Infrastructure Beans
    // ========================================================================

    /**
     * Creates executor service for async event delivery.
     */
    @Bean
    @ConditionalOnMissingBean(name = "eventFlowExecutor")
    public ExecutorService eventFlowExecutor() {
        var tp = properties.getDispatcher().getThreadPool();
        log.info("Creating EventFlow executor: core={}, max={}, queue={}",
                tp.getCoreSize(), tp.getMaxSize(), tp.getQueueCapacity());

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(tp.getCoreSize());
        taskExecutor.setMaxPoolSize(tp.getMaxSize());
        taskExecutor.setQueueCapacity(tp.getQueueCapacity());
        taskExecutor.setKeepAliveSeconds(tp.getKeepAliveSeconds());
        taskExecutor.setThreadNamePrefix("event-flow-dispatcher-");
        taskExecutor.initialize();

        return taskExecutor.getThreadPoolExecutor();
    }

    /**
     * Creates in-memory transport pair (incoming + outgoing) sharing the same queue.
     * This couples publisher and dispatcher through a shared queue.
     */
    @Bean
    @ConditionalOnMissingBean(name = "inMemoryTransports")
    @ConditionalOnProperty(prefix = "event-flow.dispatcher.transports[0]", 
        name = "type", havingValue = "in-memory", matchIfMissing = true)
    public InMemoryTransportsBuilder.InMemoryTransports inMemoryTransports(ExecutorService eventFlowExecutor) {
        var transportConfig = properties.getDispatcher().getTransports().isEmpty()
                ? new EventFlowProperties.TransportConfig()
                : properties.getDispatcher().getTransports().getFirst();

        log.info("Creating in-memory transports with capacity: {}", transportConfig.getCapacity());

        return new InMemoryTransportsBuilder()
                .queueSize(transportConfig.getCapacity())
                .executorService(eventFlowExecutor)
                .build();
    }

    // ========================================================================
    // Event Channel Beans
    // ========================================================================

    /**
     * Creates all event channels (internal + external).
     */
    @Bean
    @ConditionalOnMissingBean(name = "eventChannels")
    public List<EventChannel> eventChannels(InMemoryTransportsBuilder.InMemoryTransports inMemoryTransports) {
        
        List<EventChannel> channels = new ArrayList<>();
        
        // 1. Create internal channel (always present)
        channels.add(createInternalChannel(inMemoryTransports));
        
        // 2. Create configured channels from properties
        List<EventChannel> configuredChannels = properties.getPublisher().getChannels().stream()
            .filter(config -> !"internal".equalsIgnoreCase(config.getName()))
            .map(this::createEventChannel)
            .toList();
        
        channels.addAll(configuredChannels);
        
        log.info("Created {} event channels: {}", 
            channels.size(),
            channels.stream().map(EventChannel::name).collect(joining(", ")));
        
        return channels;
    }

    private EventChannel createInternalChannel(InMemoryTransportsBuilder.InMemoryTransports inMemoryTransports) {
        log.debug("Creating internal event channel with in-memory transport");
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
        TransportFactory factory = transportFactories.get(config.getType());
        
        if (factory == null) {
            throw new IllegalArgumentException(
                String.format("Unsupported transport type '%s' for channel '%s'. " +
                    "Supported types: %s", 
                    config.getType(), 
                    config.getName(),
                    transportFactories.keySet())
            );
        }
        
        factory.validate(config);
        return List.of(factory.createOutgoing(config));
    }

    // ========================================================================
    // Publisher Bean
    // ========================================================================

    /**
     * Creates event publisher with all configured channels.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.publisher", name = "enabled", havingValue = "true", matchIfMissing = true)
    public EventPublisher eventPublisher(List<EventChannel> eventChannels) {
        
        EventFlowProperties.PublisherConfig publisherConfig = properties.getPublisher();
        
        log.info("Configuring EventPublisher with {} channels: {}", 
            eventChannels.size(),
            eventChannels.stream()
                .map(EventChannel::name)
                .collect(joining(", ")));
        
        log.debug("Publisher configuration: transactional={}, retry={}, silent={}",
                publisherConfig.isTransactional(),
                publisherConfig.getRetry().isEnabled(),
                publisherConfig.isSilent());

        EventPublisherBuilder builder = EventPublisherBuilder.channels(eventChannels);

        // Apply retry if enabled
        var retry = publisherConfig.getRetry();
        if (retry.isEnabled()) {
            builder.retryable(retry.getMaxAttempts(), retry.getInitialDelay(), retry.getMultiplier());
        }

        // Apply silent mode if enabled
        if (publisherConfig.isSilent()) {
            builder.silent();
        }
        
        EventPublisher publisher = builder.buildAndLog();

        // Wrap with transactional decorator if enabled
        if (publisherConfig.isTransactional()) {
            log.info("Wrapping publisher with TransactionalEventPublisher");
            return new TransactionalEventPublisher(publisher);
        }
        
        return publisher;
    }

    // ========================================================================
    // Dispatcher Bean
    // ========================================================================

    /**
     * Creates event dispatcher with configured transports.
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.dispatcher", name = "enabled", havingValue = "true", matchIfMissing = true)
    public EventDispatcher eventDispatcher(
            EventListenerRegistry listenerRegistry,
            ExecutorService eventFlowExecutor,
            InMemoryTransportsBuilder.InMemoryTransports inMemoryTransports,
            List<IncomingEventTransport> incomingEventTransports) {

        log.info("Configuring EventDispatcher");

        List<IncomingEventTransport> allTransports = new ArrayList<>();
        allTransports.add(inMemoryTransports.incoming());
        allTransports.addAll(incomingEventTransports);

        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                eventFlowExecutor,
                listenerRegistry,
                allTransports
        );

        dispatcher.start();
        return dispatcher;
    }

    // ========================================================================
    // Incoming Transport Beans
    // ========================================================================

    /**
     * Creates incoming transports from configuration (excludes in-memory, handled separately).
     */
    @Bean
    @ConditionalOnMissingBean
    public List<IncomingEventTransport> incomingEventTransports() {
        return properties.getDispatcher().getTransports().stream()
            .filter(config -> !"in-memory".equalsIgnoreCase(config.getType()))
            .map(this::createIncomingTransport)
            .peek(transport -> log.info("Created incoming transport: {} ({})", 
                transport.name(), transport.getClass().getSimpleName()))
            .collect(Collectors.toList());
    }

    private IncomingEventTransport createIncomingTransport(EventFlowProperties.TransportConfig config) {
        TransportFactory factory = transportFactories.get(config.getType());
        
        if (factory == null) {
            throw new IllegalArgumentException(
                String.format("Unsupported transport type '%s' for incoming transport. " +
                    "Supported types: %s", 
                    config.getType(),
                    transportFactories.keySet())
            );
        }
        
        factory.validate(config);
        return factory.createIncoming(config);
    }

    // ========================================================================
    // Inner Classes
    // ========================================================================

    /**
     * Generic event channel for custom channel names.
     */
    private static class GenericEventChannel implements EventChannel {
        private final String name;
        private final List<OutgoingEventTransport> transports;

        public GenericEventChannel(String name, List<OutgoingEventTransport> transports) {
            this.name = name;
            this.transports = transports;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<OutgoingEventTransport> transports() {
            return transports;
        }
    }
}
