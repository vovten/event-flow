package io.github.vovten.eventflow.lifecycle.retry;

import io.github.vovten.eventflow.retry.RetryKind;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable durable retry row.
 * <p>
 * The row identifies the event and the retry domain. The event payload itself
 * lives in the event store; this row carries only scheduling and diagnostic
 * metadata.
 *
 * @param eventId         the event to retry
 * @param service         the service that owns the retry row
 * @param retryKind       delivery or processing
 * @param attempt         1-based attempt number for that kind
 * @param state           current retry state
 * @param reason          reason for the current attempt/failure
 * @param nextAttemptAt   earliest time when the retry may be claimed
 * @param lastErrorType   fully qualified class name of the last error, or null
 * @param lastError       last error message, or null
 * @param policyName      effective processing policy name, or null
 * @param claimedBy       scheduler instance that currently owns the row, or null
 * @param claimedUntil    lease expiration for the claim, or null
 * @param createdAt       when the row was first created
 * @param updatedAt       when the row was last updated
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record RetryTask(
        UUID eventId,
        String service,
        RetryKind retryKind,
        int attempt,
        RetryState state,
        RetryReason reason,
        Instant nextAttemptAt,
        String lastErrorType,
        String lastError,
        String policyName,
        String claimedBy,
        Instant claimedUntil,
        Instant createdAt,
        Instant updatedAt
) {

    public RetryTask {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(service, "service must not be null");
        if (service.isBlank()) {
            throw new IllegalArgumentException("service must not be blank");
        }
        Objects.requireNonNull(retryKind, "retryKind must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (state == RetryState.IN_PROGRESS) {
            Objects.requireNonNull(claimedBy, "claimedBy must be set for IN_PROGRESS");
            Objects.requireNonNull(claimedUntil, "claimedUntil must be set for IN_PROGRESS");
        } else if (claimedBy != null || claimedUntil != null) {
            throw new IllegalArgumentException("claim fields must be empty for " + state);
        }
    }

    /**
     * Creates a new pending retry row.
     *
     * @param eventId       the event to retry
     * @param service       the owning service
     * @param retryKind     delivery or processing
     * @param attempt       1-based attempt number
     * @param reason        reason for the retry
     * @param nextAttemptAt earliest eligible time
     * @param policyName    effective policy name, may be null
     * @return a pending retry row
     */
    public static RetryTask pending(UUID eventId, String service, RetryKind retryKind,
                                    int attempt, RetryReason reason, Instant nextAttemptAt,
                                    String policyName) {
        Instant now = Instant.now();
        return new RetryTask(eventId, service, retryKind, attempt,
                RetryState.PENDING, reason, nextAttemptAt,
                null, null, policyName, null, null, now, now);
    }

    /**
     * Returns a copy claimed by the given scheduler instance.
     *
     * @param owner        scheduler instance identifier
     * @param claimedUntil lease expiration
     * @return the claimed copy
     */
    public RetryTask withClaim(String owner, Instant claimedUntil) {
        return new RetryTask(eventId, service, retryKind, attempt,
                RetryState.IN_PROGRESS, reason, nextAttemptAt,
                lastErrorType, lastError, policyName,
                owner, claimedUntil, createdAt, Instant.now());
    }

    /**
     * Returns a copy rescheduled for a later attempt.
     *
     * @param newAttempt     new 1-based attempt number
     * @param nextAttemptAt  next eligible time
     * @param errorType      error class name, may be null
     * @param error          error message, may be null
     * @return the rescheduled copy
     */
    public RetryTask withReschedule(int newAttempt, Instant nextAttemptAt,
                                    String errorType, String error) {
        return new RetryTask(eventId, service, retryKind, newAttempt,
                RetryState.PENDING, reason, nextAttemptAt,
                errorType, error, policyName, null, null,
                createdAt, Instant.now());
    }

    /**
     * Returns a terminal dead-letter copy.
     *
     * @param deadReason reason for the terminal state
     * @param errorType  error class name, may be null
     * @param error      error message, may be null
     * @return the dead copy
     */
    public RetryTask withDead(RetryReason deadReason, String errorType, String error) {
        return new RetryTask(eventId, service, retryKind, attempt,
                RetryState.DEAD, deadReason, nextAttemptAt,
                errorType, error, policyName, null, null,
                createdAt, Instant.now());
    }

    /**
     * Returns whether this row is a pending retry that is due now.
     *
     * @param now current time
     * @return true if pending and eligible
     */
    public boolean isDue(Instant now) {
        return state == RetryState.PENDING && !nextAttemptAt.isAfter(now);
    }

    /**
     * Returns whether an in-progress claim has expired.
     *
     * @param now current time
     * @return true if the row is in progress and its lease expired
     */
    public boolean isClaimExpired(Instant now) {
        return state == RetryState.IN_PROGRESS
                && claimedUntil != null
                && !claimedUntil.isAfter(now);
    }
}
