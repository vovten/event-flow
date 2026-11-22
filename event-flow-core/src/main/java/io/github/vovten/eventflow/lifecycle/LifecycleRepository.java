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
     * Records consumer acceptance and reports whether the event was known.
     * <p>
     * A delivery confirmation is valid even when the event is already
     * {@code FAILED}/{@code HANDLED} (the status is preserved, the DELIVERY row
     * is cleared). Therefore this method returns {@code true} for any existing
     * event and {@code false} only when the event is unknown.
     *
     * @param eventId        event identifier
     * @param deliveryAttempt accepted delivery attempt
     * @return {@code true} when the event exists and the ack was processed
     */
    default boolean tryOnAccepted(UUID eventId, int deliveryAttempt) {
        onAccepted(eventId, deliveryAttempt);
        return true;
    }

    /**
     * Records consumer acceptance and optionally seeds a processing-timeout
     * watchdog row when the consumer's local retry campaign is longer than the
     * configured ack timeout.
     * <p>
     * The deadline is advisory: implementations that do not support it (the
     * default) ignore it and keep the fixed watchdog timeout. Built-in
     * repositories create a {@code PROCESSING} row at {@code processingDeadline}
     * only when the event actually transitions to {@code ACCEPTED} and no
     * processing row exists yet.
     *
     * @param eventId            event identifier
     * @param deliveryAttempt    accepted delivery attempt
     * @param service            owning service, used for the watchdog row
     * @param processingDeadline when the processing watchdog should fire, or {@code null}
     * @return {@code true} when the event exists and the ack was processed
     * @since 1.4.0
     */
    default boolean tryOnAccepted(UUID eventId, int deliveryAttempt, String service,
                                  Instant processingDeadline) {
        return tryOnAccepted(eventId, deliveryAttempt);
    }

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
     * Records a handler failure and reports whether the transition was applied.
     *
     * @param eventId       event identifier
     * @param service       owning service
     * @param attempt       attempt number that failed
     * @param retryable     whether another processing attempt is allowed
     * @param nextAttemptAt next processing retry time, used when retryable
     * @param policyName    effective policy name, may be null
     * @param errorType     error class name, may be null
     * @param error         error message, may be null
     * @return {@code true} when the event existed and the transition was applied
     */
    default boolean tryOnProcessingFailed(UUID eventId, String service, int attempt,
                                          boolean retryable, Instant nextAttemptAt,
                                          String policyName, String errorType, String error) {
        onProcessingFailed(eventId, service, attempt, retryable, nextAttemptAt,
                policyName, errorType, error);
        return true;
    }

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
     * Records a processing failure when processing retries are disabled and
     * reports whether the transition was applied.
     *
     * @param eventId   event identifier
     * @param errorType error class name, may be null
     * @param error     error message, may be null
     * @return {@code true} when the event existed and the transition was applied
     */
    default boolean tryOnProcessingDisabled(UUID eventId, String errorType, String error) {
        onProcessingDisabled(eventId, errorType, error);
        return true;
    }

    /**
     * Records successful handling and clears retry work.
     *
     * @param eventId event identifier
     * @param attempt attempt number that succeeded
     */
    void onHandled(UUID eventId, int attempt);

    /**
     * Records successful handling and reports whether the event was updated.
     * <p>
     * The default implementation delegates to {@link #onHandled(UUID, int)} and
     * returns {@code true} to preserve the legacy contract. Built-in repositories
     * override it so acknowledgments for unknown or already-terminal events are
     * not reported as successfully handled.
     *
     * @param eventId event identifier
     * @param attempt attempt number that succeeded
     * @return {@code true} when the event existed and the transition was applied
     */
    default boolean tryOnHandled(UUID eventId, int attempt) {
        onHandled(eventId, attempt);
        return true;
    }

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
     * Records that no handler was resolved and reports whether the transition
     * was applied.
     *
     * @param eventId   event identifier
     * @param service   owning service
     * @param attempt   attempt number
     * @param errorType error class name, may be null
     * @param error     error message, may be null
     * @return {@code true} when the event existed and the transition was applied
     */
    default boolean tryOnNoHandler(UUID eventId, String service, int attempt,
                                   String errorType, String error) {
        onNoHandler(eventId, service, attempt, errorType, error);
        return true;
    }

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
     * Marks the event and the retry domain terminally dead and reports whether
     * the event transition was applied.
     * <p>
     * The retry row must become terminal even when the event status is outside
     * the allowed processing sources; callers use the boolean result to force
     * the row to {@code DEAD} themselves.
     *
     * @param eventId   event identifier
     * @param service   owning service
     * @param kind      retry domain
     * @param reason    terminal reason
     * @param errorType error class name, may be null
     * @param error     error message, may be null
     * @return {@code true} when the event existed and the transition was applied
     */
    default boolean tryOnDeadLetter(UUID eventId, String service, RetryKind kind,
                                    RetryReason reason, String errorType, String error) {
        onDeadLetter(eventId, service, kind, reason, errorType, error);
        return true;
    }

    /**
     * Resets an event for manual retry.
     *
     * @param eventId event identifier
     * @param service owning service
     */
    void markForRetry(UUID eventId, String service);

    /**
     * Resets an event for manual retry and reports whether the reset was applied.
     * <p>
     * The default implementation delegates to {@link #markForRetry(UUID, String)}
     * and returns {@code true} to preserve the legacy contract. Built-in
     * repositories override it so callers can distinguish a scheduled retry
     * from an unknown event or an event that is already {@code NEW}.
     *
     * @param eventId event identifier
     * @param service owning service
     * @return {@code true} when the event existed and the transition to {@code NEW} was applied
     */
    default boolean tryMarkForRetry(UUID eventId, String service) {
        markForRetry(eventId, service);
        return true;
    }
}
