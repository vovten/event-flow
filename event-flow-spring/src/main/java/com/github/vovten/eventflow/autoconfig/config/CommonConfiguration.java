package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.dispatcher.InMemoryDispatcherTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.dispatcher.KafkaDispatcherTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.publisher.BroadcastKafkaPublisherTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.publisher.InMemoryPublisherTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.publisher.KafkaPublisherTransportFactory;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutorService;

/**
 * Auto-configuration for common components: executor service and in-memory transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class CommonConfiguration {

    private final EventFlowProperties properties;

    public CommonConfiguration(EventFlowProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates executor service for async event delivery.
     * Only created when event-flow is enabled.
     *
     * @return executor service for dispatcher
     */
    @Bean("dispatcherExecutor")
    public ExecutorService dispatcherExecutor() {
        var tp = properties.getDispatcher().getThreadPool();
        var msg = "Creating dispatcher executor: core={}, max={}, queue={}";
        log.info(msg, tp.getCoreSize(), tp.getMaxSize(), tp.getQueueCapacity());

        var taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(tp.getCoreSize());
        taskExecutor.setMaxPoolSize(tp.getMaxSize());
        taskExecutor.setQueueCapacity(tp.getQueueCapacity());
        taskExecutor.setKeepAliveSeconds(tp.getKeepAliveSeconds());
        taskExecutor.setThreadNamePrefix("event-flow-dispatcher-");
        taskExecutor.initialize();

        return taskExecutor.getThreadPoolExecutor();
    }

    /**
     * Creates queue provider for in-memory transports with configured capacity.
     * Uses capacity from first configured transport or default value.
     * Always created to support in-memory transports even when event-flow is disabled.
     *
     * @return queue provider for in-memory transports
     */
    @Bean
    @ConditionalOnMissingBean
    public DefaultQueueProvider queueProvider() {
        int capacity = properties.getDispatcher().getTransports().stream()
                .filter(config -> "in-memory".equalsIgnoreCase(config.getName()))
                .findFirst()
                .map(EventFlowProperties.TransportConfig::getCapacity)
                .orElse(1000);
        log.info("Creating QueueProvider with capacity: {}", capacity);
        return new DefaultQueueProvider(capacity);
    }

    /**
     * Creates factory for in-memory publisher transports.
     *
     * @param queueProvider queue provider for in-memory transports
     * @return in-memory publisher transport factory
     */
    @Bean
    public InMemoryPublisherTransportFactory inMemoryPublisherTransportFactory(DefaultQueueProvider queueProvider) {
        return new InMemoryPublisherTransportFactory(queueProvider);
    }

    /**
     * Creates factory for in-memory dispatcher transports.
     *
     * @param queueProvider queue provider for in-memory transports
     * @return in-memory dispatcher transport factory
     */
    @Bean
    public InMemoryDispatcherTransportFactory inMemoryDispatcherTransportFactory(DefaultQueueProvider queueProvider) {
        return new InMemoryDispatcherTransportFactory(queueProvider);
    }

    /**
     * Creates factory for Kafka publisher transports.
     *
     * @return Kafka publisher transport factory
     */
    @Bean
    public KafkaPublisherTransportFactory kafkaPublisherTransportFactory() {
        return new KafkaPublisherTransportFactory();
    }

    /**
     * Creates factory for Kafka dispatcher transports.
     *
     * @return Kafka dispatcher transport factory
     */
    @Bean
    public KafkaDispatcherTransportFactory kafkaDispatcherTransportFactory() {
        return new KafkaDispatcherTransportFactory();
    }

    /**
     * Creates factory for broadcast Kafka publisher transports.
     *
     * @return broadcast Kafka publisher transport factory
     */
    @Bean
    public BroadcastKafkaPublisherTransportFactory broadcastKafkaPublisherTransportFactory() {
        return new BroadcastKafkaPublisherTransportFactory();
    }
}
