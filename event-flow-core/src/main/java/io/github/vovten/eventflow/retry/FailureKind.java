package io.github.vovten.eventflow.retry;

/**
 * Classification of a handler failure from the processing-retry perspective.
 * <p>
 * The effective policy is always resolved: an explicit listener policy,
 * a named policy, or the configured default policy. There is no "no policy"
 * state; the ack's policy name distinguishes the default policy from explicit
 * ones.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public enum FailureKind {

    /**
     * The handler succeeded; there is no failure.
     */
    NONE,

    /**
     * The exception is not retryable according to the effective policy.
     */
    PERMANENT,

    /**
     * The exception is retryable, but the total processing attempt budget is exhausted.
     */
    TRANSIENT_EXHAUSTED
}
