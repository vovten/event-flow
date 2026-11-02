package io.github.vovten.eventflow.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RetryDelayScheduler Tests")
class RetryDelaySchedulerTest {

    private final RetryDelayScheduler scheduler = RetryDelayScheduler.defaultScheduler();

    @Test
    @DisplayName("Should complete immediately for zero or negative delays")
    void shouldCompleteImmediatelyForNonPositiveDelays() {
        CompletableFuture<Void> zero = scheduler.delay(Duration.ZERO);
        CompletableFuture<Void> negative = scheduler.delay(Duration.ofMillis(-1));
        CompletableFuture<Void> nullDelay = scheduler.delay(null);

        assertThat(zero.isDone()).isTrue();
        assertThat(negative.isDone()).isTrue();
        assertThat(nullDelay.isDone()).isTrue();
    }

    @Test
    @DisplayName("Should complete after a positive delay")
    void shouldCompleteAfterPositiveDelay() throws Exception {
        long start = System.nanoTime();

        scheduler.delay(Duration.ofMillis(100)).get(5, TimeUnit.SECONDS);

        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(50));
    }
}
