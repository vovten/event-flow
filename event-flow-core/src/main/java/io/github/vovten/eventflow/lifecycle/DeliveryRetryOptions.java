package io.github.vovten.eventflow.lifecycle;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the {@link DeliveryRetryScheduler}.
 *
 * @param service       the local service that owns the retried events (must not be blank)
 * @param interval      delay between retry cycles (must be positive)
 * @param ackTimeout    maximum time to wait for an acceptance ack (must be positive)
 * @param retryDelay    base backoff delay used when the scheduler reschedules (must not be negative)
 * @param multiplier    exponential backoff multiplier (must be &gt;= 1.0)
 * @param jitter        symmetric jitter fraction in [0, 1]
 * @param maxRetryDelay upper bound for the backoff delay (must not be shorter than {@code retryDelay})
 * @param maxRetries    maximum number of retries after the first publish (must be &gt;= 0)
 * @param batchSize     maximum number of retry rows handled per cycle (must be positive)
 * @param payloadRetryThreshold maximum number of attempts for a transient payload failure
 *                      before the event is dead-lettered (must be &gt;= 1)
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record DeliveryRetryOptions(
        String service,
        Duration interval,
        Duration ackTimeout,
        Duration retryDelay,
        double multiplier,
        double jitter,
        Duration maxRetryDelay,
        int maxRetries,
        int batchSize,
        int payloadRetryThreshold
) {

    /**
     * Creates options with the default payload retry threshold.
     *
     * @param service       the local service that owns the retried events
     * @param interval      delay between retry cycles
     * @param ackTimeout    maximum time to wait for an acceptance ack
     * @param retryDelay    base backoff delay
     * @param multiplier    exponential backoff multiplier
     * @param jitter        symmetric jitter fraction in [0, 1]
     * @param maxRetryDelay upper bound for the backoff delay
     * @param maxRetries    maximum number of retries after the first publish
     * @param batchSize     maximum number of retry rows handled per cycle
     */
    public DeliveryRetryOptions(String service, Duration interval, Duration ackTimeout,
                                Duration retryDelay, double multiplier, double jitter,
                                Duration maxRetryDelay, int maxRetries, int batchSize) {
        this(service, interval, ackTimeout, retryDelay, multiplier, jitter,
                maxRetryDelay, maxRetries, batchSize, PayloadFailure.DEFAULT_RETRY_THRESHOLD);
    }

    public DeliveryRetryOptions {
        if (service == null || service.isBlank()) {
            throw new IllegalArgumentException("service must not be null or blank");
        }
        Objects.requireNonNull(interval, "interval must not be null");
        Objects.requireNonNull(ackTimeout, "ackTimeout must not be null");
        Objects.requireNonNull(retryDelay, "retryDelay must not be null");
        Objects.requireNonNull(maxRetryDelay, "maxRetryDelay must not be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        if (ackTimeout.isZero() || ackTimeout.isNegative()) {
            throw new IllegalArgumentException("ackTimeout must be positive");
        }
        if (retryDelay.isNegative()) {
            throw new IllegalArgumentException("retryDelay must not be negative");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        if (jitter < 0.0 || jitter > 1.0) {
            throw new IllegalArgumentException("jitter must be between 0 and 1");
        }
        if (maxRetryDelay.compareTo(retryDelay) < 0) {
            throw new IllegalArgumentException("maxRetryDelay must not be shorter than retryDelay");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (payloadRetryThreshold < 1) {
            throw new IllegalArgumentException("payloadRetryThreshold must be >= 1");
        }
    }

    /**
     * Computes the exponential backoff for a failed attempt.
     * <p>
     * The delay is {@code min(retryDelay × multiplier^(attempt-1), maxRetryDelay)}.
     *
     * @param attempt 1-based attempt number that failed
     * @return the backoff delay
     * @throws IllegalArgumentException if {@code attempt} is less than 1
     */
    public Duration backoff(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        double millis = retryDelay.toMillis() * Math.pow(multiplier, attempt - 1);
        long capped = Math.min((long) millis, maxRetryDelay.toMillis());
        return Duration.ofMillis(capped);
    }
}
