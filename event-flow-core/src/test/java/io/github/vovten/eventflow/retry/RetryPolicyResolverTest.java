package io.github.vovten.eventflow.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetryPolicyResolver Tests")
class RetryPolicyResolverTest {

    private static final ResolvedRetryPolicy DEFAULTS = ResolvedRetryPolicy.defaults();

    @Test
    @DisplayName("Should inherit defaults for unset inline attributes")
    void shouldInheritDefaults() {
        ResolvedRetryPolicy policy = RetryPolicyResolver.resolveInline(retryPolicy("empty"), DEFAULTS);

        assertThat(policy).isNotNull();
        assertThat(policy.name()).isNull();
        assertThat(policy.maxRetries()).isEqualTo(DEFAULTS.maxRetries());
        assertThat(policy.localMaxRetries()).isEqualTo(DEFAULTS.localMaxRetries());
        assertThat(policy.initialDelay()).isEqualTo(DEFAULTS.initialDelay());
        assertThat(policy.multiplier()).isEqualTo(DEFAULTS.multiplier());
        assertThat(policy.maxDelay()).isEqualTo(DEFAULTS.maxDelay());
        assertThat(policy.jitter()).isEqualTo(DEFAULTS.jitter());
        assertThat(policy.retryOn()).isEmpty();
        assertThat(policy.noRetryOn()).isEmpty();
    }

    @Test
    @DisplayName("Should apply explicit inline attributes")
    void shouldApplyExplicitAttributes() {
        ResolvedRetryPolicy policy = RetryPolicyResolver.resolveInline(retryPolicy("explicit"), DEFAULTS);

        assertThat(policy.maxRetries()).isEqualTo(2);
        assertThat(policy.localMaxRetries()).isEqualTo(1);
        assertThat(policy.initialDelay()).isEqualTo(Duration.ofMillis(250));
        assertThat(policy.maxDelay()).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.multiplier()).isEqualTo(3.0);
        assertThat(policy.jitter()).isZero();
        assertThat(policy.retryOn()).containsExactly(TimeoutException.class);
        assertThat(policy.noRetryOn()).containsExactly(IllegalStateException.class);
        assertThat(policy.matches(new TimeoutException("slow"))).isTrue();
        assertThat(policy.matches(new IllegalArgumentException("bad"))).isFalse();
        assertThat(policy.backoff(1)).isEqualTo(Duration.ofMillis(250));
        assertThat(policy.backoff(2)).isEqualTo(Duration.ofMillis(750));
        assertThat(policy.backoff(3)).isEqualTo(Duration.ofMillis(2_000));
    }

    @Test
    @DisplayName("Should resolve a named policy from the registry")
    void shouldResolveNamedPolicy() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry();
        ResolvedRetryPolicy named = new ResolvedRetryPolicy(
                "payment", Set.of(), Set.of(), 5, 0,
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 0.0);
        registry.register(named);

        assertThat(RetryPolicyResolver.resolveNamed("payment", registry)).isSameAs(named);
        assertThat(RetryPolicyResolver.resolveNamed(" ", registry)).isNull();
    }

    @Test
    @DisplayName("Should prefer an inline policy over named and default policies")
    void shouldPreferInlinePolicy() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry();
        ResolvedRetryPolicy named = new ResolvedRetryPolicy(
                "payment", Set.of(), Set.of(), 5, 0,
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 0.0);
        registry.register(named);

        ResolvedRetryPolicy effective = RetryPolicyResolver.resolveEffective(
                retryPolicy("empty"), "payment", registry, DEFAULTS);

        assertThat(effective.name()).isNull();
        assertThat(effective.maxRetries()).isEqualTo(DEFAULTS.maxRetries());
        assertThat(effective).isNotSameAs(named);
    }

    @Test
    @DisplayName("Should use the named policy when no inline policy exists")
    void shouldUseNamedPolicyWhenNoInlinePolicy() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry();
        ResolvedRetryPolicy named = new ResolvedRetryPolicy(
                "payment", Set.of(), Set.of(), 5, 0,
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 0.0);
        registry.register(named);

        assertThat(RetryPolicyResolver.resolveEffective(null, "payment", registry, DEFAULTS))
                .isSameAs(named);
    }

    @Test
    @DisplayName("Should use the default policy when no policy is configured")
    void shouldUseDefaultPolicyWhenNothingIsConfigured() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry();

        assertThat(RetryPolicyResolver.resolveEffective(null, null, registry, DEFAULTS))
                .isSameAs(DEFAULTS);
    }

    @Test
    @DisplayName("Should fail fast on an unknown named policy")
    void shouldFailFastOnUnknownNamedPolicy() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry();

        assertThatThrownBy(() -> RetryPolicyResolver.resolveNamed("missing", registry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown retry policy");
    }

    @Test
    @DisplayName("Should return null when there is no policy")
    void shouldReturnNullWhenNoPolicy() {
        assertThat(RetryPolicyResolver.resolveInline(null, DEFAULTS)).isNull();
    }

    @Test
    @DisplayName("Should parse compact and ISO-8601 durations")
    void shouldParseDurations() {
        assertThat(RetryPolicyResolver.parseDuration("100ms", Duration.ZERO))
                .isEqualTo(Duration.ofMillis(100));
        assertThat(RetryPolicyResolver.parseDuration("1.5s", Duration.ZERO))
                .isEqualTo(Duration.ofMillis(1_500));
        assertThat(RetryPolicyResolver.parseDuration("2m", Duration.ZERO))
                .isEqualTo(Duration.ofMinutes(2));
        assertThat(RetryPolicyResolver.parseDuration("1h", Duration.ZERO))
                .isEqualTo(Duration.ofHours(1));
        assertThat(RetryPolicyResolver.parseDuration("1d", Duration.ZERO))
                .isEqualTo(Duration.ofDays(1));
        assertThat(RetryPolicyResolver.parseDuration("250", Duration.ZERO))
                .isEqualTo(Duration.ofMillis(250));
        assertThat(RetryPolicyResolver.parseDuration("PT0.5S", Duration.ZERO))
                .isEqualTo(Duration.ofMillis(500));
    }

    @Test
    @DisplayName("Should use the default for a blank duration")
    void shouldUseDefaultForBlankDuration() {
        Duration fallback = Duration.ofSeconds(3);

        assertThat(RetryPolicyResolver.parseDuration(null, fallback)).isEqualTo(fallback);
        assertThat(RetryPolicyResolver.parseDuration("  ", fallback)).isEqualTo(fallback);
    }

    @Test
    @DisplayName("Should reject invalid durations")
    void shouldRejectInvalidDurations() {
        assertThatThrownBy(() -> RetryPolicyResolver.parseDuration("abc", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid duration");

        assertThatThrownBy(() -> RetryPolicyResolver.parseDuration("-1s", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }

    private static RetryPolicy retryPolicy(String methodName) {
        try {
            return Annotated.class.getDeclaredMethod(methodName).getAnnotation(RetryPolicy.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError("Missing annotated method: " + methodName, e);
        }
    }

    @SuppressWarnings("unused")
    private static final class Annotated {

        @RetryPolicy
        private void empty() {
        }

        @RetryPolicy(
                maxRetries = 2,
                localMaxRetries = 1,
                initialDelay = "250ms",
                maxDelay = "2s",
                multiplier = 3.0,
                jitter = 0.0,
                retryOn = TimeoutException.class,
                noRetryOn = IllegalStateException.class
        )
        private void explicit() {
        }
    }
}
