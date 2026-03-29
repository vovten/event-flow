package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.incoming.LocalQueueInTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.incoming.KafkaInTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.BroadcastKafkaOutTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.InMemoryOutTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.KafkaOutTransportFactory;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
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
    public DefaultLocalQueueProvider queueProvider() {
        int capacity = properties.getDispatcher().getTransports().stream()
                .filter(config -> "in-memory".equalsIgnoreCase(config.getName()))
                .findFirst()
                .map(EventFlowProperties.TransportConfig::getCapacity)
                .orElse(1000);
        log.info("Creating QueueProvider with capacity: {}", capacity);
        return new DefaultLocalQueueProvider(capacity);
    }

    /**
     * Creates factory for in-memory publisher transports.
     *
     * @param queueProvider queue provider for in-memory transports
     * @return in-memory publisher transport factory
     */
    @Bean
    public InMemoryOutTransportFactory inMemoryPublisherTransportFactory(DefaultLocalQueueProvider queueProvider) {
        return new InMemoryOutTransportFactory(queueProvider);
    }

    /**
     * Creates factory for in-memory dispatcher transports.
     *
     * @param queueProvider queue provider for in-memory transports
     * @return in-memory dispatcher transport factory
     */
    @Bean
    public LocalQueueInTransportFactory inMemoryDispatcherTransportFactory(DefaultLocalQueueProvider queueProvider) {
        return new LocalQueueInTransportFactory(queueProvider);
    }

    /**
     * Creates factory for Kafka publisher transports.
     *
     * @return Kafka publisher transport factory
     */
    @Bean
    public KafkaOutTransportFactory kafkaPublisherTransportFactory() {
        return new KafkaOutTransportFactory();
    }

    /**
     * Creates factory for Kafka dispatcher transports.
     *
     * @return Kafka dispatcher transport factory
     */
    @Bean
    public KafkaInTransportFactory kafkaDispatcherTransportFactory() {
        return new KafkaInTransportFactory();
    }

    /**
     * Creates factory for broadcast Kafka publisher transports.
     *
     * @return broadcast Kafka publisher transport factory
     */
    @Bean
    public BroadcastKafkaOutTransportFactory broadcastKafkaPublisherTransportFactory() {
        return new BroadcastKafkaOutTransportFactory();
    }
}
