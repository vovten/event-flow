package io.github.vovten.eventflow.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ResolvedRetryPolicy Tests")
class ResolvedRetryPolicyTest {

    @Test
    @DisplayName("Should expose backward-compatible global defaults")
    void shouldExposeGlobalDefaults() {
        ResolvedRetryPolicy policy = ResolvedRetryPolicy.defaults();

        assertThat(policy.name()).isEqualTo(ResolvedRetryPolicy.DEFAULT_POLICY_NAME);
        assertThat(policy.retryOn()).isEmpty();
        assertThat(policy.noRetryOn()).isEmpty();
        assertThat(policy.maxRetries()).isEqualTo(3);
        assertThat(policy.localMaxRetries()).isZero();
        assertThat(policy.initialDelay()).isEqualTo(Duration.ofSeconds(30));
        assertThat(policy.multiplier()).isEqualTo(2.0);
        assertThat(policy.maxDelay()).isEqualTo(Duration.ofMinutes(10));
        assertThat(policy.jitter()).isEqualTo(0.5);
        assertThat(policy.hasRetries()).isTrue();
    }

    @Test
    @DisplayName("Should reject a blank policy name")
    void shouldRejectBlankName() {
        assertThatThrownBy(() -> new ResolvedRetryPolicy(
                " ", Set.of(), Set.of(), 1, 0,
                Duration.ofMillis(1), 2.0, Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name");
    }

    @Test
    @DisplayName("Should reject invalid configuration")
    void shouldRejectInvalidConfiguration() {
        assertThatThrownBy(() -> policy(-1, 0, Duration.ofMillis(1), 2.0, Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries");

        assertThatThrownBy(() -> policy(3, -1, Duration.ofMillis(1), 2.0, Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("localMaxRetries");

        assertThatThrownBy(() -> policy(2, 3, Duration.ofMillis(1), 2.0, Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("localMaxRetries must not exceed");

        assertThatThrownBy(() -> policy(1, 0, Duration.ofMillis(-1), 2.0, Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialDelay");

        assertThatThrownBy(() -> policy(1, 0, Duration.ofMillis(1), 0.5, Duration.ofSeconds(1), 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier");

        assertThatThrownBy(() -> policy(1, 0, Duration.ofMillis(1), 2.0, Duration.ofMillis(-1), 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDelay");

        assertThatThrownBy(() -> policy(1, 0, Duration.ofMillis(1), 2.0, Duration.ofSeconds(1), 1.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitter");
    }

    @Test
    @DisplayName("Should decide between local and durable retries")
    void shouldDecideBetweenLocalAndDurableRetries() {
        ResolvedRetryPolicy policy = policy(5, 2, Duration.ofMillis(100), 2.0, Duration.ofSeconds(1), 0.0);

        assertThat(policy.isLocalRetryAllowed(1)).isTrue();
        assertThat(policy.isLocalRetryAllowed(2)).isTrue();
        assertThat(policy.isLocalRetryAllowed(3)).isFalse();
        assertThat(policy.hasMoreAttempts(5)).isTrue();
        assertThat(policy.hasMoreAttempts(6)).isFalse();
    }

    @Test
    @DisplayName("Should support durable-only and local-only modes")
    void shouldSupportDurableOnlyAndLocalOnlyModes() {
        ResolvedRetryPolicy durableOnly = policy(3, 0, Duration.ofMillis(100), 2.0, Duration.ofSeconds(1), 0.0);
        ResolvedRetryPolicy localOnly = policy(3, 3, Duration.ofMillis(100), 2.0, Duration.ofSeconds(1), 0.0);

        assertThat(durableOnly.isLocalRetryAllowed(1)).isFalse();
        assertThat(durableOnly.hasMoreAttempts(1)).isTrue();
        assertThat(localOnly.isLocalRetryAllowed(3)).isTrue();
        assertThat(localOnly.isLocalRetryAllowed(4)).isFalse();
    }

    @Test
    @DisplayName("Should match any exception when retryOn is empty")
    void shouldMatchAnyExceptionWhenRetryOnEmpty() {
        ResolvedRetryPolicy policy = policy(3, 0, Duration.ofMillis(1), 2.0, Duration.ofSeconds(1), 0.0);

        assertThat(policy.matches(new IllegalStateException("boom"))).isTrue();
    }

    @Test
    @DisplayName("Should match retryOn types and inspect the cause chain")
    void shouldMatchRetryOnAndCauseChain() {
        ResolvedRetryPolicy policy = new ResolvedRetryPolicy(
                "test", Set.of(TimeoutException.class), Set.of(), 3, 0,
                Duration.ofMillis(1), 2.0, Duration.ofSeconds(1), 0.0);

        assertThat(policy.matches(new TimeoutException("slow"))).isTrue();
        assertThat(policy.matches(new IllegalStateException("boom"))).isFalse();
        assertThat(policy.matches(new RuntimeException(new TimeoutException("slow")))).isTrue();
        assertThat(policy.matches(null)).isFalse();
    }

    @Test
    @DisplayName("Should let noRetryOn win over retryOn")
    void shouldLetNoRetryOnWin() {
        ResolvedRetryPolicy policy = new ResolvedRetryPolicy(
                "test", Set.of(RuntimeException.class), Set.of(IllegalArgumentException.class), 3, 0,
                Duration.ofMillis(1), 2.0, Duration.ofSeconds(1), 0.0);

        assertThat(policy.matches(new IllegalStateException("boom"))).isTrue();
        assertThat(policy.matches(new IllegalArgumentException("bad"))).isFalse();
    }

    @Test
    @DisplayName("Should compute capped exponential backoff when jitter is disabled")
    void shouldComputeCappedExponentialBackoff() {
        ResolvedRetryPolicy policy = new ResolvedRetryPolicy(
                "test", Set.of(), Set.of(), 5, 2,
                Duration.ofMillis(100), 2.0, Duration.ofMillis(1_000), 0.0);

        assertThat(policy.backoff(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.backoff(2)).isEqualTo(Duration.ofMillis(200));
        assertThat(policy.backoff(3)).isEqualTo(Duration.ofMillis(400));
        assertThat(policy.backoff(4)).isEqualTo(Duration.ofMillis(800));
        assertThat(policy.backoff(5)).isEqualTo(Duration.ofMillis(1_000));
    }

    @Test
    @DisplayName("Should apply full jitter within the capped delay")
    void shouldApplyFullJitter() {
        ResolvedRetryPolicy policy = new ResolvedRetryPolicy(
                "test", Set.of(), Set.of(), 1, 0,
                Duration.ofMillis(100), 2.0, Duration.ofMillis(1_000), 1.0);

        assertThat(policy.backoff(1))
                .isBetween(Duration.ZERO, Duration.ofMillis(100));
    }

    @Test
    @DisplayName("Should reject a retry number below one")
    void shouldRejectInvalidRetryNumber() {
        ResolvedRetryPolicy policy = ResolvedRetryPolicy.defaults();

        assertThatThrownBy(() -> policy.backoff(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryNumber");
    }

    private static ResolvedRetryPolicy policy(int maxRetries, int localMaxRetries,
                                              Duration initialDelay, double multiplier,
                                              Duration maxDelay, double jitter) {
        return new ResolvedRetryPolicy("test", Set.of(), Set.of(), maxRetries, localMaxRetries,
                initialDelay, multiplier, maxDelay, jitter);
    }
}
