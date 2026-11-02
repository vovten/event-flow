package io.github.vovten.eventflow.retry;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Schedules a non-blocking delay between local retry attempts.
 * <p>
 * The dispatcher uses this abstraction instead of {@link Thread#sleep(long)} so
 * that platform-thread workers are not blocked during backoff. Tests can supply
 * an immediate scheduler.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@FunctionalInterface
public interface RetryDelayScheduler {

    /**
     * Returns a future that completes after the given delay.
     * <p>
     * A {@code null}, zero or negative delay completes immediately.
     *
     * @param delay the delay before the next attempt
     * @return a future that completes when the delay has elapsed
     */
    CompletableFuture<Void> delay(Duration delay);

    /**
     * Returns the default scheduler backed by {@link CompletableFuture#delayedExecutor(long, TimeUnit)}.
     *
     * @return the default non-blocking delay scheduler
     */
    static RetryDelayScheduler defaultScheduler() {
        return delay -> {
            if (delay == null || delay.isZero() || delay.isNegative()) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.runAsync(
                    () -> { },
                    CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS));
        };
    }
}
