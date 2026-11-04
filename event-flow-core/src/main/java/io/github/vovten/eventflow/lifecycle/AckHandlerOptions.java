package io.github.vovten.eventflow.lifecycle;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the retry-aware {@link AckHandler}.
 *
 * @param processingEnabled     whether processing retries are enabled
 * @param defaultProcessingDelay fallback delay used when a {@code FailureAck} has no relative delay
 * @param maxProcessingDelay    upper clamp for a consumer-provided retry delay (must be positive)
 * @param maxProcessingRetries  global hard cap for processing retries after the first attempt
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record AckHandlerOptions(
        boolean processingEnabled,
        Duration defaultProcessingDelay,
        Duration maxProcessingDelay,
        int maxProcessingRetries
) {

    public AckHandlerOptions {
        Objects.requireNonNull(defaultProcessingDelay, "defaultProcessingDelay must not be null");
        Objects.requireNonNull(maxProcessingDelay, "maxProcessingDelay must not be null");
        if (defaultProcessingDelay.isNegative()) {
            throw new IllegalArgumentException("defaultProcessingDelay must not be negative");
        }
        if (maxProcessingDelay.isZero() || maxProcessingDelay.isNegative()) {
            throw new IllegalArgumentException("maxProcessingDelay must be positive");
        }
        if (maxProcessingDelay.compareTo(defaultProcessingDelay) < 0) {
            throw new IllegalArgumentException(
                    "maxProcessingDelay must not be shorter than defaultProcessingDelay");
        }
        if (maxProcessingRetries < 0) {
            throw new IllegalArgumentException("maxProcessingRetries must be >= 0");
        }
    }
}
