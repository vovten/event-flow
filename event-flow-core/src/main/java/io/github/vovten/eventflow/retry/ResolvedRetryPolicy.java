package io.github.vovten.eventflow.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Immutable, fully resolved processing-retry policy.
 * <p>
 * The policy describes one attempt sequence. The first {@link #localMaxRetries()}
 * retries are performed locally in the dispatcher; the remaining retries are
 * performed through durable re-publication. A value of {@code 0} means
 * "durable-only"; a value equal to {@link #maxRetries()} means "local-only".
 * <p>
 * Resolution from {@link RetryPolicy} annotations, named policies and the
 * configured default policy is performed by {@link RetryPolicyResolver}.
 *
 * @param name             unique policy name
 * @param retryOn          retryable exception types; empty means "any exception"
 * @param noRetryOn        non-retryable exception types; wins over {@code retryOn}
 * @param maxRetries       total retries after the first attempt (local + durable)
 * @param localMaxRetries  retries performed locally before durable re-publication
 * @param initialDelay     delay before the first retry
 * @param multiplier       exponential backoff multiplier
 * @param maxDelay         upper bound for the backoff delay
 * @param jitter           jitter fraction in {@code [0, 1]}; {@code 1} means full jitter
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record ResolvedRetryPolicy(
        String name,
        Set<Class<? extends Throwable>> retryOn,
        Set<Class<? extends Throwable>> noRetryOn,
        int maxRetries,
        int localMaxRetries,
        Duration initialDelay,
        double multiplier,
        Duration maxDelay,
        double jitter
) {

    /**
     * Reserved name of the configured default policy.
     */
    public static final String DEFAULT_POLICY_NAME = "default";

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_LOCAL_MAX_RETRIES = 0;
    private static final Duration DEFAULT_INITIAL_DELAY = Duration.ofSeconds(30);
    private static final double DEFAULT_MULTIPLIER = 2.0;
    private static final Duration DEFAULT_MAX_DELAY = Duration.ofMinutes(10);
    private static final double DEFAULT_JITTER = 0.5;

    public ResolvedRetryPolicy {
        Objects.requireNonNull(name, "name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(retryOn, "retryOn must not be null");
        Objects.requireNonNull(noRetryOn, "noRetryOn must not be null");
        Objects.requireNonNull(initialDelay, "initialDelay must not be null");
        Objects.requireNonNull(maxDelay, "maxDelay must not be null");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        if (localMaxRetries < 0) {
            throw new IllegalArgumentException("localMaxRetries must be >= 0");
        }
        if (localMaxRetries > maxRetries) {
            throw new IllegalArgumentException("localMaxRetries must not exceed maxRetries");
        }
        if (initialDelay.isNegative()) {
            throw new IllegalArgumentException("initialDelay must not be negative");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        if (maxDelay.isNegative()) {
            throw new IllegalArgumentException("maxDelay must not be negative");
        }
        if (jitter < 0.0 || jitter > 1.0) {
            throw new IllegalArgumentException("jitter must be in [0, 1]");
        }
        retryOn = Set.copyOf(retryOn);
        noRetryOn = Set.copyOf(noRetryOn);
    }

    /**
     * Returns the default policy used when a listener has no explicit policy
     * and as the source of unset annotation attributes.
     * <p>
     * The shipped defaults preserve legacy behaviour: no local retries, three
     * durable retries, 30-second initial delay.
     *
     * @return the default resolved policy
     */
    public static ResolvedRetryPolicy defaults() {
        return new ResolvedRetryPolicy(
                DEFAULT_POLICY_NAME,
                Set.of(),
                Set.of(),
                DEFAULT_MAX_RETRIES,
                DEFAULT_LOCAL_MAX_RETRIES,
                DEFAULT_INITIAL_DELAY,
                DEFAULT_MULTIPLIER,
                DEFAULT_MAX_DELAY,
                DEFAULT_JITTER);
    }

    /**
     * Checks whether the given error matches this policy.
     * <p>
     * The whole cause chain is inspected. {@link #noRetryOn()} wins over
     * {@link #retryOn()}. An empty {@link #retryOn()} means "any exception".
     *
     * @param error the handler failure, may be {@code null}
     * @return {@code true} if another attempt is allowed by the exception filter
     */
    public boolean matches(Throwable error) {
        if (error == null) {
            return false;
        }
        if (matchesAny(error, noRetryOn)) {
            return false;
        }
        return retryOn.isEmpty() || matchesAny(error, retryOn);
    }

    /**
     * Returns whether another attempt is allowed by the total retry budget.
     *
     * @param currentAttempt the 1-based attempt number that just failed
     * @return {@code true} if {@code currentAttempt <= maxRetries}
     */
    public boolean hasMoreAttempts(int currentAttempt) {
        return currentAttempt >= 1 && currentAttempt <= maxRetries;
    }

    /**
     * Returns whether the next retry should be performed locally.
     *
     * @param currentAttempt the 1-based attempt number that just failed
     * @return {@code true} if {@code currentAttempt <= localMaxRetries}
     */
    public boolean isLocalRetryAllowed(int currentAttempt) {
        return currentAttempt >= 1 && currentAttempt <= localMaxRetries;
    }

    /**
     * Returns whether this policy allows at least one retry.
     *
     * @return {@code true} if {@code maxRetries > 0}
     */
    public boolean hasRetries() {
        return maxRetries > 0;
    }

    /**
     * Computes the jittered backoff for the given retry number.
     *
     * @param retryNumber 1-based retry number (1 = first retry)
     * @return the delay before the next attempt
     * @throws IllegalArgumentException if {@code retryNumber < 1}
     */
    public Duration backoff(int retryNumber) {
        if (retryNumber < 1) {
            throw new IllegalArgumentException("retryNumber must be >= 1");
        }
        double raw = initialDelay.toMillis() * Math.pow(multiplier, retryNumber - 1.0);
        long capped = Math.min((long) raw, maxDelay.toMillis());
        double factor = 1.0 - jitter + ThreadLocalRandom.current().nextDouble() * jitter;
        return Duration.ofMillis(Math.max(0L, (long) (capped * factor)));
    }

    private static boolean matchesAny(Throwable error, Set<Class<? extends Throwable>> types) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            for (Class<? extends Throwable> type : types) {
                if (type.isInstance(current)) {
                    return true;
                }
            }
        }
        return false;
    }
}
