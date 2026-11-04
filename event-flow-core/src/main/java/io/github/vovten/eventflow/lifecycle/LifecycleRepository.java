package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.retry.RetryKind;

import java.time.Instant;
import java.util.UUID;

/**
 * Transactional facade for coordinated {@code event_store} and
 * {@code event_retry} lifecycle transitions.
 * <p>
 * The event status is the coarse aggregate; the retry table holds pending work.
 * Implementations must update both sides atomically for a single transition.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public interface LifecycleRepository {

    /**
     * Records a publish failure and schedules a delivery retry.
     *
     * @param eventId       event identifier
     * @param service       owning service
     * @param reason        retry reason
     * @param nextAttemptAt earliest retry time
     * @param errorType     error class name, may be null
     * @param error         error message, may be null
     */
    void onPublishFailed(UUID eventId, String service, RetryReason reason,
                         Instant nextAttemptAt, String errorType, String error);

    /**
     * Records consumer acceptance and clears the delivery retry.
     *
     * @param eventId        event identifier
     * @param deliveryAttempt accepted delivery attempt
     */
    void onAccepted(UUID eventId, int deliveryAttempt);

    /**
     * Records a handler failure.
     *
     * @param eventId       event identifier
     * @param service       owning service
     * @param attempt       attempt number that failed
     * @param retryable     whether another processing attempt is allowed
     * @param nextAttemptAt next processing retry time, used when retryable
     * @param policyName    effective policy name, may be null
     * @param errorType     error class name, may be null
     * @param error         error message, may be null
     */
    void onProcessingFailed(UUID eventId, String service, int attempt, boolean retryable,
                            Instant nextAttemptAt, String policyName, String errorType, String error);

    /**
     * Records a processing failure when processing retries are disabled.
     * <p>
     * The event becomes {@code FAILED} with error details, the delivery retry
     * is cleared, and no processing retry row is created.
     *
     * @param eventId   event identifier
     * @param errorType error class name, may be null
     * @param error     error message, may be null
     */
    void onProcessingDisabled(UUID eventId, String errorType, String error);

    /**
     * Records successful handling and clears retry work.
     *
     * @param eventId event identifier
     * @param attempt attempt number that succeeded
     */
    void onHandled(UUID eventId, int attempt);

    /**
     * Records that no handler was resolved; this is a terminal configuration failure.
     *
     * @param eventId   event identifier
     * @param service   owning service
     * @param attempt   attempt number
     * @param errorType error class name, may be null
     * @param error     error message, may be null
     */
    void onNoHandler(UUID eventId, String service, int attempt, String errorType, String error);

    /**
     * Marks the event and the given retry domain as terminally dead.
     *
     * @param eventId   event identifier
     * @param service   owning service
     * @param kind      retry domain
     * @param reason    terminal reason
     * @param errorType error class name, may be null
     * @param error     error message, may be null
     */
    void onDeadLetter(UUID eventId, String service, RetryKind kind, RetryReason reason,
                      String errorType, String error);

    /**
     * Resets an event for manual retry.
     *
     * @param eventId event identifier
     * @param service owning service
     */
    void markForRetry(UUID eventId, String service);
}
