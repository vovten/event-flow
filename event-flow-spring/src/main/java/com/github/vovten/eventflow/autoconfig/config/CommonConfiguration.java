package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.incoming.KafkaInTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.incoming.LocalQueueInTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.BroadcastKafkaOutTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.KafkaOutTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.LocalQueueOutTransportFactory;
import com.github.vovten.eventflow.serialization.EventSerializerFactory;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Auto-configuration for common components: executor service and local-queue transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@DependsOn("serializerRegistrationComplete")
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true")
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
        var msg = "Creating dispatcher executor: core={}, max={}, queue={}, rejection-policy=caller-runs";
        log.info(msg, tp.getCoreSize(), tp.getMaxSize(), tp.getQueueCapacity());

        var taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(tp.getCoreSize());
        taskExecutor.setMaxPoolSize(tp.getMaxSize());
        taskExecutor.setQueueCapacity(tp.getQueueCapacity());
        taskExecutor.setKeepAliveSeconds(tp.getKeepAliveSeconds());
        taskExecutor.setThreadNamePrefix("event-flow-dispatcher-");
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskExecutor.initialize();

        return taskExecutor.getThreadPoolExecutor();
    }

    /**
     * Creates queue provider for local-queue transports with configured capacity.
     * Uses capacity from first configured transport or default value.
     * Always created to support local-queue transports even when event-flow is disabled.
     *
     * @return queue provider for local-queue transports
     */
    @Bean
    @ConditionalOnMissingBean
    public DefaultLocalQueueProvider queueProvider() {
        int capacity = properties.getDispatcher().getTransports().stream()
                .filter(config -> "local-queue".equalsIgnoreCase(config.getName()))
                .findFirst()
                .map(EventFlowProperties.TransportConfig::getCapacity)
                .orElse(1000);
        log.info("Creating QueueProvider with capacity: {}", capacity);
        return new DefaultLocalQueueProvider(capacity);
    }

    /**
     * Creates factory for local-queue publisher transports.
     *
     * @param queueProvider queue provider for local-queue transports
     * @return local-queue publisher transport factory
     */
    @Bean
    public LocalQueueOutTransportFactory localQueuePublisherTransportFactory(DefaultLocalQueueProvider queueProvider) {
        return new LocalQueueOutTransportFactory(queueProvider);
    }

    /**
     * Creates factory for local-queue dispatcher transports.
     *
     * @param queueProvider queue provider for local-queue transports
     * @return local-queue dispatcher transport factory
     */
    @Bean
    public LocalQueueInTransportFactory localQueueDispatcherTransportFactory(DefaultLocalQueueProvider queueProvider) {
        return new LocalQueueInTransportFactory(queueProvider);
    }

    /**
     * Creates factory for Kafka publisher transports.
     *
     * @param serializerFactory serializer factory for creating event serializers
     * @return Kafka publisher transport factory
     */
    @Bean
    public KafkaOutTransportFactory kafkaPublisherTransportFactory(EventSerializerFactory serializerFactory) {
        return new KafkaOutTransportFactory(serializerFactory);
    }

    /**
     * Creates factory for Kafka dispatcher transports.
     *
     * @param serializerFactory serializer factory for creating event serializers
     * @return Kafka dispatcher transport factory
     */
    @Bean
    public KafkaInTransportFactory kafkaDispatcherTransportFactory(EventSerializerFactory serializerFactory) {
        return new KafkaInTransportFactory(serializerFactory);
    }

    /**
     * Creates factory for broadcast Kafka publisher transports.
     *
     * @param serializerFactory serializer factory for creating event serializers
     * @return broadcast Kafka publisher transport factory
     */
    @Bean
    public BroadcastKafkaOutTransportFactory broadcastKafkaPublisherTransportFactory(EventSerializerFactory serializerFactory) {
        return new BroadcastKafkaOutTransportFactory(serializerFactory);
    }
}
