package io.github.vovten.eventflow.lifecycle;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the {@link ProcessingRetryScheduler}.
 *
 * @param service       the local service that owns the retried events (must not be blank)
 * @param interval      delay between retry cycles (must be positive)
 * @param ackTimeout    maximum time to wait for a final ack (must be positive)
 * @param retryDelay    base backoff delay used when the scheduler reschedules (must not be negative)
 * @param multiplier    exponential backoff multiplier (must be &gt;= 1.0)
 * @param maxRetryDelay upper bound for the backoff delay (must not be shorter than {@code retryDelay})
 * @param maxRetries    maximum number of retries after the first attempt (must be &gt;= 0)
 * @param batchSize     maximum number of retry rows handled per cycle (must be positive)
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record ProcessingRetryOptions(
        String service,
        Duration interval,
        Duration ackTimeout,
        Duration retryDelay,
        double multiplier,
        Duration maxRetryDelay,
        int maxRetries,
        int batchSize
) {

    public ProcessingRetryOptions {
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
        if (maxRetryDelay.compareTo(retryDelay) < 0) {
            throw new IllegalArgumentException("maxRetryDelay must not be shorter than retryDelay");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
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
