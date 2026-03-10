package com.github.vovten.eventflow.autoconfig;

import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.publisher.EventPublisher;
import com.github.vovten.eventflow.publisher.EventPublisherBuilder;
import com.github.vovten.eventflow.publisher.SilentEventPublisher;
import com.github.vovten.eventflow.publisher.TransactionalEventPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * Auto-configuration for event publisher.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EventFlowPublisherConfiguration {

    private final EventFlowProperties properties;

    public EventFlowPublisherConfiguration(EventFlowProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates event publisher with all configured channels.
     */
    @Bean
    @ConditionalOnMissingBean
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

        EventPublisher publisher = builder.buildAndLog();

        // Wrap with transactional decorator if enabled
        if (publisherConfig.isTransactional()) {
            log.info("Wrapping publisher with TransactionalEventPublisher");
            publisher = new TransactionalEventPublisher(publisher);
        }

        // Apply silent mode as the outermost decorator (suppresses all exceptions)
        if (publisherConfig.isSilent()) {
            log.info("Wrapping publisher with SilentEventPublisher");
            publisher = new SilentEventPublisher(publisher);
        }

        return publisher;
    }
}
