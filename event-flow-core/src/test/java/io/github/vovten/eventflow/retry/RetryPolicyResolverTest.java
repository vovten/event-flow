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
    @DisplayName("Should inherit defaults for unset declared attributes")
    void shouldInheritDefaults() {
        ResolvedRetryPolicy policy = RetryPolicyResolver.resolveDeclared(retryPolicy("empty"), DEFAULTS);

        assertThat(policy).isNotNull();
        assertThat(policy.name()).isEqualTo("empty");
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
    @DisplayName("Should apply explicit declared attributes")
    void shouldApplyExplicitAttributes() {
        ResolvedRetryPolicy policy = RetryPolicyResolver.resolveDeclared(retryPolicy("explicit"), DEFAULTS);

        assertThat(policy.name()).isEqualTo("explicit");
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
    @DisplayName("Should reject a blank policy name")
    void shouldRejectBlankName() {
        assertThatThrownBy(() -> RetryPolicyResolver.resolveDeclared(retryPolicy("blank"), DEFAULTS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name");
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
    @DisplayName("Should use the named policy when no method declaration exists")
    void shouldUseNamedPolicyWhenNoDeclaration() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry();
        ResolvedRetryPolicy named = new ResolvedRetryPolicy(
                "payment", Set.of(), Set.of(), 5, 0,
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 0.0);
        registry.register(named);

        assertThat(RetryPolicyResolver.resolveEffective(null, "payment", registry, DEFAULTS))
                .isSameAs(named);
    }

    @Test
    @DisplayName("Should use the default policy when nothing is configured")
    void shouldUseDefaultPolicyWhenNothingIsConfigured() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry();

        assertThat(RetryPolicyResolver.resolveEffective(null, null, registry, DEFAULTS))
                .isSameAs(DEFAULTS);
        assertThat(DEFAULTS.name()).isEqualTo(ResolvedRetryPolicy.DEFAULT_POLICY_NAME);
    }

    @Test
    @DisplayName("Should reject a method declaration combined with a policy reference")
    void shouldRejectDeclarationAndReferenceTogether() {
        RetryPolicyRegistry registry = new RetryPolicyRegistry();

        assertThatThrownBy(() -> RetryPolicyResolver.resolveEffective(
                retryPolicy("empty"), "payment", registry, DEFAULTS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Both");
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
    @DisplayName("Should return null when there is no declaration")
    void shouldReturnNullWhenNoDeclaration() {
        assertThat(RetryPolicyResolver.resolveDeclared(null, DEFAULTS)).isNull();
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

        @RetryPolicy(name = "empty")
        private void empty() {
        }

        @RetryPolicy(
                name = "explicit",
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

        @RetryPolicy(name = "  ")
        private void blank() {
        }
    }
}
