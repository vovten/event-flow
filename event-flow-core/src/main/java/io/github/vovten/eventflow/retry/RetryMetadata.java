package io.github.vovten.eventflow.retry;

/**
 * Envelope metadata keys used to correlate acknowledgments with the retry
 * context of the dispatch that produced them.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetryMetadata {

    /**
     * Retry context of the dispatch: {@link RetryKind#DELIVERY} or
     * {@link RetryKind#PROCESSING}.
     */
    public static final String RETRY_KIND = "retryKind";

    /**
     * 1-based attempt number within the retry context.
     */
    public static final String ATTEMPT = "attempt";

    private RetryMetadata() {
    }
}
