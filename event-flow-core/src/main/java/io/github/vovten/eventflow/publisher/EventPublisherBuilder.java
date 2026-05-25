package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.channel.EventChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for creating configured {@link EventPublisher} instances.
 * <p>
 * Allows flexible composition of different publisher features:
 * <ul>
 *   <li>Channel configuration</li>
 *   <li>Retry with exponential backoff</li>
 *   <li>Custom decorators</li>
 * </ul>
 * <p>
 * <b>Usage examples:</b>
 * <pre>{@code
 * // Simple publisher with channels only
 * EventPublisher publisher = EventPublisherBuilder.create(channel1, channel2)
 *     .build();
 *
 * // Publisher with retry support
 * EventPublisher publisher = EventPublisherBuilder.create(channels)
 *     .retryable(3, Duration.ofMillis(100), 2.0)
 *     .build();
 *
 * // Complete configuration with custom decorator
 * EventPublisher publisher = EventPublisherBuilder.create(channels)
 *     .retryable(5, Duration.ofSeconds(1), 1.5)
 *     .withDecorator(pub -> new MetricsEventPublisher(pub, metricsRegistry))
 *     .build();
 * }</pre>
 * <p>
 * <b>Order of decorators:</b>
 * The builder applies decorators in the following order (from innermost to outermost):
 * <ol>
 *   <li>Base {@link ChannelEventPublisher}</li>
 *   <li>Custom decorators (applied in order added)</li>
 *   <li>{@link RetryEventPublisher} (if enabled)</li>
 *   <li>Subclass decorations via {@link #decorate(EventPublisher)} — always outermost</li>
 * </ol>
 * <p>
 * <b>Note:</b> For transactional publishing (defer until after transaction commit),
 * use the Spring integration module (event-flow-spring) which provides
 * {@code SpringEventPublisherBuilder} with {@code transactional()} support.
 *
 * @param <T> the concrete builder type (CRTP pattern for fluent interface)
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class EventPublisherBuilder<T extends EventPublisherBuilder<T>> {

    private static final Logger log = LoggerFactory.getLogger(EventPublisherBuilder.class);

    private RetryConfig retryConfig;
    private final List<EventChannel> channels = new ArrayList<>();
    private final List<DecoratorFunction> decorators = new ArrayList<>();

    protected EventPublisherBuilder() {
    }

    /**
     * Start building a new EventPublisher.
     *
     * @return builder instance
     */
    public static EventPublisherBuilder<?> create() {
        return new EventPublisherBuilder<>();
    }

    /**
     * Start building publisher with the given channels.
     *
     * @param channels event channels to configure
     * @return builder instance
     */
    public static EventPublisherBuilder<?> create(EventChannel... channels) {
        return new EventPublisherBuilder<>().addChannels(channels);
    }

    /**
     * Start building publisher with the given channels.
     *
     * @param channels list of event channels
     * @return builder instance
     */
    public static EventPublisherBuilder<?> create(List<EventChannel> channels) {
        return new EventPublisherBuilder<>().addChannels(channels);
    }

    /**
     * Add more channels to the configuration.
     *
     * @param channels channels to add
     * @return this builder
     */
    @SuppressWarnings("unchecked")
    public T addChannels(EventChannel... channels) {
        this.channels.addAll(List.of(channels));
        return (T) this;
    }

    /**
     * Add more channels to the configuration.
     *
     * @param channels channels to add
     * @return this builder
     */
    @SuppressWarnings("unchecked")
    public T addChannels(List<EventChannel> channels) {
        this.channels.addAll(channels);
        return (T) this;
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
    @SuppressWarnings("unchecked")
    public T retryable() {
        this.retryConfig = new RetryConfig(3, Duration.ofMillis(100), 2.0);
        return (T) this;
    }

    /**
     * Enable retry with custom settings.
     *
     * @param maxRetries maximum number of retry attempts
     * @param initialDelay initial delay between retries
     * @param multiplier backoff multiplier
     * @return this builder
     */
    @SuppressWarnings("unchecked")
    public T retryable(int maxRetries, Duration initialDelay, double multiplier) {
        this.retryConfig = new RetryConfig(maxRetries, initialDelay, multiplier);
        return (T) this;
    }

    /**
     * Add a custom decorator to the publisher chain.
     * Decorators are applied in the order they are added.
     *
     * @param decorator function that transforms an EventPublisher into a decorated one
     * @return this builder
     */
    @SuppressWarnings("unchecked")
    public T withDecorator(DecoratorFunction decorator) {
        this.decorators.add(decorator);
        return (T) this;
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

        // Allow subclasses to add additional decorations (e.g., transactional, logging) — always outermost
        publisher = decorate(publisher);
        return publisher;
    }

    /**
     * Hook for subclasses to add additional decorations.
     * <p>
     * Default implementation returns the publisher unchanged.
     *
     * @param publisher the current publisher in the chain
     * @return decorated publisher (may be the same instance or a new one)
     */
    protected EventPublisher decorate(EventPublisher publisher) {
        return publisher;
    }

    /**
     * Build and return the publisher, logging the final configuration.
     *
     * @return configured EventPublisher
     */
    public EventPublisher buildAndLog() {
        EventPublisher publisher = build();
        log.info("Built EventPublisher with configuration: channels={}, retry={}, customDecorators={}",
                channels.size(),
                retryConfig != null ? "enabled" : "disabled",
                decorators.size()
        );
        return publisher;
    }

    protected int getChannelsSize() {
        return channels.size();
    }

    RetryConfig getRetryConfig() {
        return retryConfig;
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
