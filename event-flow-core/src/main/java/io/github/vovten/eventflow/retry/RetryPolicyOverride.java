package io.github.vovten.eventflow.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * Partial override for a named processing-retry policy.
 * <p>
 * {@code null} fields leave the corresponding value unchanged. The override is
 * applied to a policy declaration before it is registered.
 *
 * @param maxRetries       total retry budget, or {@code null}
 * @param localMaxRetries  local retry budget, or {@code null}
 * @param initialDelay     initial backoff delay, or {@code null}
 * @param multiplier       backoff multiplier, or {@code null}
 * @param maxDelay         maximum backoff delay, or {@code null}
 * @param jitter           jitter fraction, or {@code null}
 * @param retryOn          retryable exception types, or {@code null}
 * @param noRetryOn        non-retryable exception types, or {@code null}
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record RetryPolicyOverride(
        Integer maxRetries,
        Integer localMaxRetries,
        Duration initialDelay,
        Double multiplier,
        Duration maxDelay,
        Double jitter,
        Set<Class<? extends Throwable>> retryOn,
        Set<Class<? extends Throwable>> noRetryOn
) {

    public RetryPolicyOverride {
        if (retryOn != null) {
            retryOn = Set.copyOf(retryOn);
        }
        if (noRetryOn != null) {
            noRetryOn = Set.copyOf(noRetryOn);
        }
    }

    /**
     * Applies this override to the given policy.
     *
     * @param base the declared policy
     * @return the effective policy
     */
    public ResolvedRetryPolicy applyTo(ResolvedRetryPolicy base) {
        Objects.requireNonNull(base, "base must not be null");
        return new ResolvedRetryPolicy(
                base.name(),
                retryOn != null ? retryOn : base.retryOn(),
                noRetryOn != null ? noRetryOn : base.noRetryOn(),
                maxRetries != null ? maxRetries : base.maxRetries(),
                localMaxRetries != null ? localMaxRetries : base.localMaxRetries(),
                initialDelay != null ? initialDelay : base.initialDelay(),
                multiplier != null ? multiplier : base.multiplier(),
                maxDelay != null ? maxDelay : base.maxDelay(),
                jitter != null ? jitter : base.jitter());
    }
}
