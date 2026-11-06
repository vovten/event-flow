package io.github.vovten.eventflow.lifecycle;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the {@link RetryWatchdog}.
 *
 * @param service               the local service whose events are watched (must not be blank)
 * @param interval              delay between watchdog cycles (must be positive)
 * @param deliveryAckTimeout    age after which a {@code PUBLISHED} event is considered stale
 * @param processingAckTimeout  age after which an {@code ACCEPTED} event is considered stale
 * @param batchSize             maximum number of stale events handled per cycle (must be positive)
 * @param deliveryEnabled       whether the delivery watchdog is active
 * @param processingEnabled     whether the processing watchdog is active
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record RetryWatchdogOptions(
        String service,
        Duration interval,
        Duration deliveryAckTimeout,
        Duration processingAckTimeout,
        int batchSize,
        boolean deliveryEnabled,
        boolean processingEnabled
) {

    public RetryWatchdogOptions {
        if (service == null || service.isBlank()) {
            throw new IllegalArgumentException("service must not be null or blank");
        }
        Objects.requireNonNull(interval, "interval must not be null");
        Objects.requireNonNull(deliveryAckTimeout, "deliveryAckTimeout must not be null");
        Objects.requireNonNull(processingAckTimeout, "processingAckTimeout must not be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        if (deliveryAckTimeout.isZero() || deliveryAckTimeout.isNegative()) {
            throw new IllegalArgumentException("deliveryAckTimeout must be positive");
        }
        if (processingAckTimeout.isZero() || processingAckTimeout.isNegative()) {
            throw new IllegalArgumentException("processingAckTimeout must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
    }
}
