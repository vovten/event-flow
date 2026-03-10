package com.github.vovten.eventflow.autoconfig;

import com.github.vovten.eventflow.transport.InMemoryTransportsBuilder;
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
public class EventFlowCommonConfiguration {

    private final EventFlowProperties properties;

    public EventFlowCommonConfiguration(EventFlowProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates executor service for async event delivery.
     */
    @Bean
    @ConditionalOnMissingBean(name = "eventFlowExecutor")
    public ExecutorService dispatcherExecutor() {
        var tp = properties.getDispatcher().getThreadPool();
        log.info("Creating dispatcher executor: core={}, max={}, queue={}",
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
}
