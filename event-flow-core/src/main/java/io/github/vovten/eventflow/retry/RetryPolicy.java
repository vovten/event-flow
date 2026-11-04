package io.github.vovten.eventflow.retry;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a named processing-retry policy.
 * <p>
 * There is one policy concept: a named policy. The annotation can be placed
 * in two locations:
 * <ul>
 *   <li>on a listener method — declares the named policy and binds it to that
 *       listener;</li>
 *   <li>on a type — declares the named policy for reuse; listeners reference
 *       it with {@code @EventListener(retryPolicy = "...")}.</li>
 * </ul>
 * A listener with neither a method-level declaration nor a reference uses the
 * configured default policy. Setting both a method-level annotation and a
 * {@code retryPolicy} reference is ambiguous and fails fast.
 * <p>
 * Attributes left at their sentinel values inherit the default policy:
 * <ul>
 *   <li>{@link #maxRetries()} {@code < 0} — inherit;</li>
 *   <li>{@link #localMaxRetries()} {@code < 0} — inherit;</li>
 *   <li>{@link #initialDelay()} blank — inherit;</li>
 *   <li>{@link #maxDelay()} blank — inherit;</li>
 *   <li>{@link #multiplier()} {@code < 1.0} — inherit;</li>
 *   <li>{@link #jitter()} {@code < 0} — inherit.</li>
 * </ul>
 * An empty {@link #retryOn()} means "retry any exception". {@link #noRetryOn()}
 * always wins over {@link #retryOn()}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface RetryPolicy {

    /**
     * Unique policy name. Required.
     *
     * @return the policy name
     */
    String name();

    /**
     * Exception types that are retried. Empty means "any exception".
     *
     * @return retryable exception types
     */
    Class<? extends Throwable>[] retryOn() default {};

    /**
     * Exception types that are never retried. Wins over {@link #retryOn()}.
     *
     * @return non-retryable exception types
     */
    Class<? extends Throwable>[] noRetryOn() default {};

    /**
     * Total number of retries after the first attempt across the local and
     * durable layers.
     *
     * @return the total retry budget, or {@code -1} to inherit the default policy
     */
    int maxRetries() default -1;

    /**
     * Number of retries performed locally in the dispatcher before handing the
     * event over to durable re-publication.
     * <p>
     * {@code 0} means "durable-only": no local retries. The value must not
     * exceed {@link #maxRetries()}. A negative value inherits the default policy.
     *
     * @return the local retry budget, or {@code -1} to inherit the default policy
     */
    int localMaxRetries() default -1;

    /**
     * Delay before the first local retry, for example {@code "100ms"} or {@code "PT0.1S"}.
     *
     * @return the initial delay, or an empty string to inherit the global default
     */
    String initialDelay() default "";

    /**
     * Upper bound for the exponential backoff.
     *
     * @return the maximum delay, or an empty string to inherit the global default
     */
    String maxDelay() default "";

    /**
     * Backoff multiplier. Values below {@code 1.0} inherit the global default.
     *
     * @return the multiplier, or a negative value to inherit the global default
     */
    double multiplier() default -1.0;

    /**
     * Jitter fraction in the range {@code [0, 1]}. Negative values inherit the
     * global default; {@code 1.0} means full jitter.
     *
     * @return the jitter fraction, or a negative value to inherit the global default
     */
    double jitter() default -1.0;
}
