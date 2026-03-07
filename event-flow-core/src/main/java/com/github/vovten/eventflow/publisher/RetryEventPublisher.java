package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Decorator for {@link EventPublisher} that adds automatic retry support for failed event publishing.
 * <p>
 * When event publishing fails, this decorator will automatically retry the operation with
 * exponential backoff delay between attempts. This helps handle transient failures such as
 * network issues, temporary broker unavailability, or connection timeouts.
 * <p>
 * <b>Retry strategy:</b>
 * <ul>
 *   <li>Uses exponential backoff: delay = initialDelay * (multiplier ^ (attempt - 1))</li>
 *   <li>Only retries transient exceptions (network, timeout), not configuration errors</li>
 *   <li>Logs each retry attempt with delay information</li>
 *   <li>After all retries exhausted, throws the last exception</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * EventPublisher basePublisher = new ChannelEventPublisher(channels);
 * EventPublisher retryPublisher = new RetryEventPublisher(
 *     basePublisher,
 *     3,                              // max 3 retries
 *     Duration.ofMillis(100),         // initial delay 100ms
 *     2                               // multiplier 2x (100ms → 200ms → 400ms)
 * );
 * retryPublisher.publish(event);
 * }</pre>
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Unreliable network connections</li>
 *   <li>Message brokers that may have temporary outages</li>
 *   <li>Critical events that must be delivered</li>
 * </ul>
 * <p>
 * <b>When NOT to use:</b>
 * <ul>
 *   <li>High-throughput scenarios where latency is critical</li>
 *   <li>Events that can be safely lost</li>
 *   <li>When using transactional outbox pattern instead</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @see ChannelEventPublisher
 * @since 2026-03-05
 */
@Slf4j
public class RetryEventPublisher implements EventPublisher {

    private final EventPublisher origin;
    private final int maxRetries;
    private final Duration initialDelay;
    private final double multiplier;

    /**
     * Create retry decorator with default settings.
     * <ul>
     *   <li>Max retries: 3</li>
     *   <li>Initial delay: 100ms</li>
     *   <li>Multiplier: 2.0 (exponential backoff)</li>
     * </ul>
     *
     * @param origin the delegate publisher to wrap
     */
    public RetryEventPublisher(EventPublisher origin) {
        this(origin, 3, Duration.ofMillis(100), 2.0);
    }

    /**
     * Create retry decorator with custom settings.
     *
     * @param origin       the origin publisher to wrap
     * @param maxRetries   maximum number of retry attempts (must be >= 0)
     * @param initialDelay initial delay between retries
     * @param multiplier   backoff multiplier (must be >= 1.0)
     * @throws IllegalArgumentException if maxRetries < 0 or multiplier < 1.0
     */
    public RetryEventPublisher(EventPublisher origin, int maxRetries, Duration initialDelay, double multiplier) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("Max retries must be >= 0");
        }
        if (initialDelay.isNegative() || initialDelay.isZero()) {
            throw new IllegalArgumentException("Initial delay must be positive");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("Multiplier must be >= 1.0");
        }
        this.origin = origin;
        this.maxRetries = maxRetries;
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
    }

    @Override
    public void publish(Event event) {
        Exception lastException = null;
        for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
            try {
                origin.publish(event);
                if (attempt > 1) {
                    log.debug("Event {} published successfully after {} attempts",
                            event.type().getSimpleName(), attempt);
                }
                return;
            } catch (Exception e) {
                lastException = e;
                if (!shouldRetry(e)) {
                    log.warn("Non-retryable error publishing event {}: {}",
                            event.type().getSimpleName(), e.getMessage());
                    throw e;
                }
                if (attempt <= maxRetries) {
                    long delayMs = calculateDelay(attempt);
                    log.warn("Failed to publish event {} (attempt {}/{}). Retrying in {}ms: {}",
                            event.type().getSimpleName(), attempt, maxRetries + 1, delayMs, e.getMessage());
                    sleep(delayMs);
                } else {
                    log.error("Failed to publish event {} after {} attempts",
                            event.type().getSimpleName(), maxRetries + 1, e);
                }
            }
        }
        String text = "Failed to publish event %s after %d attempts";
        String msg = String.format(text, event.type().getSimpleName(), maxRetries + 1);
        throw new EventPublisherException(msg, lastException);
    }

    /**
     * Calculate delay for the given attempt using exponential backoff.
     *
     * @param attempt the current attempt number (1-based)
     * @return delay in milliseconds
     */
    private long calculateDelay(int attempt) {
        double delay = initialDelay.toMillis() * Math.pow(multiplier, attempt - 1);
        return (long) Math.min(delay, 10000); // Cap at 10 seconds
    }

    /**
     * Determine if the exception is retryable.
     * <p>
     * Retryable exceptions include:
     * <ul>
     *   <li>Network exceptions</li>
     *   <li>Timeout exceptions</li>
     *   <li>Temporary broker unavailability</li>
     * </ul>
     * <p>
     * Non-retryable exceptions include:
     * <ul>
     *   <li>{@link EventPublisherConfigException} - configuration errors</li>
     *   <li>{@link IllegalArgumentException} - invalid arguments</li>
     * </ul>
     *
     * @param e the exception to check
     * @return true if the exception should trigger a retry
     */
    private boolean shouldRetry(Exception e) {
        if (e instanceof EventPublisherConfigException) {
            return false;
        }
        if (e instanceof IllegalArgumentException) {
            return false;
        }
        return true;
    }

    /**
     * Sleep for the specified duration.
     * Package-private for testing.
     *
     * @param millis the duration to sleep in milliseconds
     */
    void sleep(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublisherException("Retry interrupted", e);
        }
    }
}
