package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.channel.EventChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Spring-aware fluent builder for creating configured {@link EventPublisher} instances.
 * <p>
 * Extends the core {@link EventPublisherBuilder} with Spring-specific features:
 * <ul>
 *   <li>Transactional publishing (defer until after transaction commit)</li>
 *   <li>Integration with Spring transaction management</li>
 * </ul>
 * <p>
 * <b>Usage examples:</b>
 * <pre>{@code
 * // Simple transactional publisher
 * EventPublisher publisher = SpringEventPublisherBuilder.channels(channel1, channel2)
 *     .transactional()
 *     .build();
 *
 * // Transactional publisher with retry
 * EventPublisher publisher = SpringEventPublisherBuilder.channels(channels)
 *     .transactional()
 *     .withRetry(3, Duration.ofMillis(100), 2.0)
 *     .build();
 *
 * // Transactional publisher with silent mode
 * EventPublisher publisher = SpringEventPublisherBuilder.channels(channels)
 *     .transactional()
 *     .withRetry()
 *     .silent()
 *     .build();
 * }</pre>
 * <p>
 * <b>Order of decorators:</b>
 * The builder applies decorators in the following order (from innermost to outermost):
 * <ol>
 *   <li>Base {@link ChannelEventPublisher}</li>
 *   <li>Custom decorators (applied in order added)</li>
 *   <li>{@link RetryEventPublisher} (if enabled)</li>
 *   <li>{@link TransactionalEventPublisher} (if enabled)</li>
 *   <li>{@link SilentEventPublisher} (if enabled) — always outermost</li>
 * </ol>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-31
 * @see EventPublisherBuilder
 */
@Slf4j
public final class SpringEventPublisherBuilder extends EventPublisherBuilder {

    private boolean transactional = false;

    /**
     * Start building publisher with the given channels.
     *
     * @param channels event channels to configure
     * @return builder instance
     */
    public static SpringEventPublisherBuilder channels(EventChannel... channels) {
        return new SpringEventPublisherBuilder().addChannels(channels);
    }

    /**
     * Start building publisher with the given channels.
     *
     * @param channels list of event channels
     * @return builder instance
     */
    public static SpringEventPublisherBuilder channels(List<EventChannel> channels) {
        return new SpringEventPublisherBuilder().addChannels(channels);
    }

    /**
     * Add more channels to the configuration.
     *
     * @param channels channels to add
     * @return this builder
     */
    public SpringEventPublisherBuilder addChannels(EventChannel... channels) {
        super.addChannels(channels);
        return this;
    }

    /**
     * Add more channels to the configuration.
     *
     * @param channels channels to add
     * @return this builder
     */
    public SpringEventPublisherBuilder addChannels(List<EventChannel> channels) {
        super.addChannels(channels);
        return this;
    }

    /**
     * Enable transactional publishing.
     * Events will be published only after the current transaction commits.
     * Uses Spring's {@link TransactionalEventPublisher}.
     *
     * @return this builder
     */
    public SpringEventPublisherBuilder transactional() {
        this.transactional = true;
        return this;
    }

    @Override
    protected EventPublisher decorate(EventPublisher publisher) {
        if (transactional) {
            log.debug("Applying transactional decorator");
            return new TransactionalEventPublisher(publisher);
        }
        return publisher;
    }

    /**
     * Build and return the publisher, logging the final configuration.
     *
     * @return configured EventPublisher
     */
    @Override
    public EventPublisher buildAndLog() {
        EventPublisher publisher = build();
        log.info("Built SpringEventPublisher with configuration: channels={}, transactional={}, retry={}, silent={}",
                getChannelsSize(),
                transactional,
                getRetryConfig() != null ? "enabled" : "disabled",
                isSilent()
        );
        return publisher;
    }
}
