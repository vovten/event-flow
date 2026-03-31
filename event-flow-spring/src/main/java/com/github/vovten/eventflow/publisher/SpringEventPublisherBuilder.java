package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.channel.EventChannel;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
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
public final class SpringEventPublisherBuilder {

    private boolean transactional = false;
    private boolean silent = false;
    private RetryConfig retryConfig;
    private final List<EventChannel> channels = new ArrayList<>();
    private final List<DecoratorFunction> decorators = new ArrayList<>();

    private SpringEventPublisherBuilder() {
    }

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
        this.channels.addAll(List.of(channels));
        return this;
    }

    /**
     * Add more channels to the configuration.
     *
     * @param channels channels to add
     * @return this builder
     */
    public SpringEventPublisherBuilder addChannels(List<EventChannel> channels) {
        this.channels.addAll(channels);
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
     * Enable retry with default settings:
     * <ul>
     *   <li>max retries: 3</li>
     *   <li>initial delay: 100ms</li>
     *   <li>multiplier: 2.0</li>
     * </ul>
     *
     * @return this builder
     */
    public SpringEventPublisherBuilder retryable() {
        this.retryConfig = new RetryConfig(3, Duration.ofMillis(100), 2.0);
        return this;
    }

    /**
     * Enable retry with custom settings.
     *
     * @param maxRetries maximum number of retry attempts
     * @param initialDelay initial delay between retries
     * @param multiplier backoff multiplier
     * @return this builder
     */
    public SpringEventPublisherBuilder retryable(int maxRetries, Duration initialDelay, double multiplier) {
        this.retryConfig = new RetryConfig(maxRetries, initialDelay, multiplier);
        return this;
    }

    /**
     * Enable silent mode (catch and log all exceptions).
     * When enabled, the publisher will never throw exceptions.
     *
     * @return this builder
     */
    public SpringEventPublisherBuilder silent() {
        this.silent = true;
        return this;
    }

    /**
     * Add a custom decorator to the publisher chain.
     * Decorators are applied in the order they are added.
     *
     * @param decorator function that transforms an EventPublisher into a decorated one
     * @return this builder
     */
    public SpringEventPublisherBuilder withDecorator(DecoratorFunction decorator) {
        this.decorators.add(decorator);
        return this;
    }

    /**
     * Build the EventPublisher instance with all configured features.
     *
     * @return configured EventPublisher
     * @throws IllegalStateException if no channels configured
     */
    public EventPublisher build() {
        if (channels.isEmpty()) {
            throw new IllegalStateException("At least one channel must be configured");
        }

        // Start with base publisher
        EventPublisher publisher = new ChannelEventPublisher(channels);

        // Apply custom decorators (innermost first)
        for (DecoratorFunction decorator : decorators) {
            publisher = decorator.apply(publisher);
            log.debug("Applied custom decorator: {}", decorator.getClass().getSimpleName());
        }

        // Apply retry if configured
        if (retryConfig != null) {
            publisher = new RetryEventPublisher(
                    publisher,
                    retryConfig.maxRetries,
                    retryConfig.initialDelay,
                    retryConfig.multiplier
            );
            log.debug("Applied retry decorator with maxRetries={}, initialDelay={}, multiplier={}",
                    retryConfig.maxRetries, retryConfig.initialDelay, retryConfig.multiplier);
        }

        // Apply transactional decorator if configured
        if (transactional) {
            publisher = new TransactionalEventPublisher(publisher);
            log.debug("Applied transactional decorator");
        }

        // Apply silent last (outermost) if configured
        if (silent) {
            publisher = new SilentEventPublisher(publisher);
            log.debug("Applied silent decorator");
        }

        return publisher;
    }

    /**
     * Build and return the publisher, logging the final configuration.
     *
     * @return configured EventPublisher
     */
    public EventPublisher buildAndLog() {
        EventPublisher publisher = build();
        log.info("Built SpringEventPublisher with configuration: channels={}, transactional={}, retry={}, silent={}, customDecorators={}",
                channels.size(),
                transactional,
                retryConfig != null ? "enabled" : "disabled",
                silent,
                decorators.size()
        );
        return publisher;
    }

    /**
     * Functional interface for custom decorators.
     */
    @FunctionalInterface
    public interface DecoratorFunction {
        /**
         * Applies decorator to the given publisher.
         *
         * @param publisher event publisher to decorate
         * @return decorated event publisher
         */
        EventPublisher apply(EventPublisher publisher);
    }

    /**
     * Internal configuration class for retry settings.
     */
    private static class RetryConfig {
        final int maxRetries;
        final Duration initialDelay;
        final double multiplier;

        RetryConfig(int maxRetries, Duration initialDelay, double multiplier) {
            this.maxRetries = maxRetries;
            this.initialDelay = initialDelay;
            this.multiplier = multiplier;
        }
    }
}
