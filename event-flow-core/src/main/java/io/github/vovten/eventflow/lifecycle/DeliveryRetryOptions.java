package io.github.vovten.eventflow.lifecycle;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the {@link DeliveryRetryScheduler}.
 *
 * @param service    the local service that owns the retried events (must not be blank)
 * @param interval   delay between retry cycles (must be positive)
 * @param ackTimeout maximum time to wait for an acceptance ack (must be positive)
 * @param maxRetries maximum number of retries after the first publish (must be &gt;= 0)
 * @param batchSize  maximum number of retry rows handled per cycle (must be positive)
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record DeliveryRetryOptions(
        String service,
        Duration interval,
        Duration ackTimeout,
        int maxRetries,
        int batchSize
) {

    public DeliveryRetryOptions {
        if (service == null || service.isBlank()) {
            throw new IllegalArgumentException("service must not be null or blank");
        }
        Objects.requireNonNull(interval, "interval must not be null");
        Objects.requireNonNull(ackTimeout, "ackTimeout must not be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        if (ackTimeout.isZero() || ackTimeout.isNegative()) {
            throw new IllegalArgumentException("ackTimeout must be positive");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
    }
}
