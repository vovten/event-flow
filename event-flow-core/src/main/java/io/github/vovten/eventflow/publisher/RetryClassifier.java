package io.github.vovten.eventflow.publisher;

/**
 * Decides whether a publishing failure may succeed on a retry.
 * <p>
 * {@link RetryEventPublisher} consults the classifier before every retry. The
 * default implementation, {@link DefaultRetryClassifier}, retries transient
 * failures and refuses deterministic ones such as configuration and programmer
 * errors, serialization failures and oversized records.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@FunctionalInterface
public interface RetryClassifier {

    /**
     * Returns whether the failure may disappear on a retry.
     *
     * @param error the failure, or {@code null} when only error details are
     *              available (for example a full local queue)
     * @return true when the failure is retryable
     */
    boolean shouldRetry(Throwable error);
}
