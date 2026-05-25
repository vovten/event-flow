package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.channel.EventChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * EventPublisher publisher = SpringEventPublisherBuilder.create(channel1, channel2)
 *     .transactional()
 *     .build();
 *
 * // Transactional publisher with retry
 * EventPublisher publisher = SpringEventPublisherBuilder.create(channels)
 *     .transactional()
 *     .retryable(3, Duration.ofMillis(100), 2.0)
 *     .build();
 * }</pre>
 * <p>
 * <b>Order of decorators:</b>
 * The builder applies decorators in the following order (from innermost to outermost):
 * <ol>
 *   <li>Base {@link ChannelEventPublisher}</li>
 *   <li>Custom decorators (applied in order added)</li>
 *   <li>{@link RetryEventPublisher} (if enabled)</li>
 *   <li>{@link LoggingEventPublisher} (if enabled)</li>
 *   <li>{@link TransactionalEventPublisher} (if enabled) — always outermost</li>
 * </ol>
 * <p>
 * <b>Important: Transactional publishing limitations</b>
 * <p>
 * See {@link TransactionalEventPublisher} for important notes about thread safety
 * and transaction boundaries. Do not use blocking operations like {@code .join()}
 * or {@code .get()} inside {@code @Transactional} methods.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see EventPublisherBuilder
 * @see TransactionalEventPublisher
 */
public final class SpringEventPublisherBuilder extends EventPublisherBuilder<SpringEventPublisherBuilder> {

    private static final Logger log = LoggerFactory.getLogger(SpringEventPublisherBuilder.class);

    private boolean transactional = false;
    private boolean loggable = false;
    private int logMaxPayloadLength = 500;

    /**
     * Start building a new SpringEventPublisher.
     *
     * @return builder instance
     */
    public static SpringEventPublisherBuilder create() {
        return new SpringEventPublisherBuilder();
    }

    /**
     * Start building publisher with the given channels.
     *
     * @param channels event channels to configure
     * @return builder instance
     */
    public static SpringEventPublisherBuilder create(EventChannel... channels) {
        return new SpringEventPublisherBuilder().addChannels(channels);
    }

    /**
     * Start building publisher with the given channels.
     *
     * @param channels list of event channels
     * @return builder instance
     */
    public static SpringEventPublisherBuilder create(List<EventChannel> channels) {
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

    /**
     * Enable logging of published events at INFO level.
     *
     * @param maxPayloadLength maximum length of payload in log output
     * @return this builder
     */
    public SpringEventPublisherBuilder loggable(int maxPayloadLength) {
        this.loggable = true;
        this.logMaxPayloadLength = maxPayloadLength;
        return this;
    }

    /**
     * Enable logging of published events at INFO level with default max payload length.
     *
     * @return this builder
     */
    public SpringEventPublisherBuilder loggable() {
        return loggable(500);
    }

    @Override
    protected EventPublisher decorate(EventPublisher publisher) {
        EventPublisher result = publisher;

        if (loggable) {
            log.debug("Applying logging decorator with maxPayloadLength={}", logMaxPayloadLength);
            result = new LoggingEventPublisher(result, logMaxPayloadLength);
        }

        if (transactional) {
            log.debug("Applying transactional decorator");
            result = new TransactionalEventPublisher(result);
        }

        return result;
    }

    /**
     * Build and return the publisher, logging the final configuration.
     *
     * @return configured EventPublisher
     */
    @Override
    public EventPublisher buildAndLog() {
        EventPublisher publisher = build();
        log.info("Built SpringEventPublisher with configuration: channels={}, transactional={}, loggable={}, retry={}",
                getChannelsSize(),
                transactional,
                loggable,
                getRetryConfig() != null ? "enabled" : "disabled"
        );
        return publisher;
    }

    public boolean isLoggable() {
        return loggable;
    }
}