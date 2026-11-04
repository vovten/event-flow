package io.github.vovten.eventflow.lifecycle.retry;

import io.github.vovten.eventflow.retry.RetryKind;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence SPI for the durable retry queue.
 * <p>
 * One row exists per {@code (eventId, retryKind)}. The row is the source of
 * truth for pending retry work; the event payload remains in the event store.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public interface EventRetryStore {

    /**
     * Inserts or replaces a retry row.
     *
     * @param task the retry task
     */
    void schedule(RetryTask task);

    /**
     * Finds retry rows that are due for the given kind and service.
     * <p>
     * Implementations should include pending rows whose {@code nextAttemptAt}
     * is not after {@code now} and may include in-progress rows whose lease has
     * expired. Results are ordered by {@code nextAttemptAt} and limited to
     * {@code batchSize}.
     *
     * @param retryKind delivery or processing
     * @param service   owning service
     * @param now       current time
     * @param batchSize maximum number of rows
     * @return due retry rows (never null)
     */
    List<RetryTask> findDue(RetryKind retryKind, String service, Instant now, int batchSize);

    /**
     * Atomically claims a retry row.
     *
     * @param eventId      event identifier
     * @param retryKind    delivery or processing
     * @param owner        scheduler instance identifier
     * @param claimedUntil lease expiration
     * @return true if the row was claimed, false if it was already claimed or not due
     */
    boolean claim(UUID eventId, RetryKind retryKind, String owner, Instant claimedUntil);

    /**
     * Reschedules a claimed row for another attempt.
     *
     * @param eventId       event identifier
     * @param retryKind     delivery or processing
     * @param attempt       new 1-based attempt number
     * @param nextAttemptAt next eligible time
     * @param errorType     error class name, may be null
     * @param error         error message, may be null
     */
    void reschedule(UUID eventId, RetryKind retryKind, int attempt, Instant nextAttemptAt,
                    String errorType, String error);

    /**
     * Deletes a retry row after a successful outcome.
     *
     * @param eventId   event identifier
     * @param retryKind delivery or processing
     */
    void complete(UUID eventId, RetryKind retryKind);

    /**
     * Marks a retry row as terminally dead.
     *
     * @param eventId   event identifier
     * @param retryKind delivery or processing
     * @param reason    terminal reason
     * @param errorType error class name, may be null
     * @param error     error message, may be null
     */
    void dead(UUID eventId, RetryKind retryKind, RetryReason reason, String errorType, String error);

    /**
     * Finds a retry row.
     *
     * @param eventId   event identifier
     * @param retryKind delivery or processing
     * @return the row, or empty when absent
     */
    Optional<RetryTask> find(UUID eventId, RetryKind retryKind);

    /**
     * Deletes all retry rows for the given event identifiers.
     *
     * @param eventIds event identifiers
     * @return number of deleted rows
     */
    int deleteByEventIds(Collection<UUID> eventIds);
}
