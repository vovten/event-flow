package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.retry.RetryKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Shared claim, lease, reschedule and dead-letter operations for the durable
 * retry schedulers.
 * <p>
 * Both {@link DeliveryRetryScheduler} and {@link ProcessingRetryScheduler}
 * delegate the common retry-store interactions to this support object. Sharing
 * a single instance is optional; the schedulers coordinate through the retry
 * store, not through in-memory state.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetrySchedulerSupport {

    private static final Logger log = LoggerFactory.getLogger(RetrySchedulerSupport.class);

    private final EventRetryStore retryStore;
    private final LifecycleRepository lifecycleRepository;
    private final String owner;

    /**
     * Creates a support object.
     *
     * @param retryStore          durable retry store
     * @param lifecycleRepository transactional lifecycle facade for dead-lettering
     * @param owner               scheduler instance identifier used in claims
     */
    public RetrySchedulerSupport(EventRetryStore retryStore,
                                 LifecycleRepository lifecycleRepository,
                                 String owner) {
        this.retryStore = Objects.requireNonNull(retryStore, "retryStore must not be null");
        this.lifecycleRepository = Objects.requireNonNull(lifecycleRepository,
                "lifecycleRepository must not be null");
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("owner must not be null or blank");
        }
        this.owner = owner;
    }

    /**
     * Finds due retry rows for the given kind and service.
     *
     * @param retryKind delivery or processing
     * @param service   owning service
     * @param now       current time
     * @param batchSize maximum number of rows
     * @return due retry rows
     */
    public List<RetryTask> findDue(RetryKind retryKind, String service, Instant now, int batchSize) {
        return retryStore.findDue(retryKind, service, now, batchSize);
    }

    /**
     * Finds a retry row of the given kind.
     *
     * @param eventId   event identifier
     * @param retryKind delivery or processing
     * @return the row, or empty when absent
     */
    public Optional<RetryTask> find(UUID eventId, RetryKind retryKind) {
        return retryStore.find(eventId, retryKind);
    }

    /**
     * Claims the row after moving it to the given attempt.
     *
     * @param task       the row to advance and claim
     * @param nextAttempt next 1-based attempt number
     * @param now        current time
     * @param lease      claim lease duration
     * @return true if this instance claimed the row
     */
    public boolean claimNextAttempt(RetryTask task, int nextAttempt, Instant now, Duration lease) {
        retryStore.schedule(task.withNextAttempt(nextAttempt, task.reason(), now, null, null));
        return claim(task, now, lease);
    }

    /**
     * Claims the row at its current attempt.
     *
     * @param task  the row to claim
     * @param now   current time
     * @param lease claim lease duration
     * @return true if this instance claimed the row
     */
    public boolean claim(RetryTask task, Instant now, Duration lease) {
        return retryStore.claim(task.eventId(), task.retryKind(), owner, now.plus(lease));
    }

    /**
     * Reschedules the row for the next attempt.
     *
     * @param task          the row to reschedule
     * @param nextAttempt   next 1-based attempt number
     * @param reason        reason for the next attempt
     * @param nextAttemptAt next eligible time
     * @param errorType     error class name, may be null
     * @param error         error message, may be null
     */
    public void reschedule(RetryTask task, int nextAttempt, RetryReason reason,
                           Instant nextAttemptAt, String errorType, String error) {
        retryStore.schedule(task.withNextAttempt(nextAttempt, reason, nextAttemptAt, errorType, error));
    }

    /**
     * Marks the event and its retry row as terminally dead.
     *
     * @param task   the retry row
     * @param reason terminal reason
     * @param error  error message, may be null
     */
    public void deadLetter(RetryTask task, RetryReason reason, String error) {
        try {
            lifecycleRepository.onDeadLetter(task.eventId(), task.service(),
                    task.retryKind(), reason, null, error);
            log.error("Dead-lettered {} retry for event {}: {}",
                    task.retryKind(), task.eventId(), error);
        } catch (RuntimeException e) {
            log.error("Failed to dead-letter {} retry for event {}",
                    task.retryKind(), task.eventId(), e);
        }
    }

    /**
     * Marks only the retry row as dead when the stored event is missing.
     *
     * @param task  the retry row
     * @param error error message
     */
    public void markDeadWithoutEvent(RetryTask task, String error) {
        log.error("Cannot retry {} for event {}: {}", task.retryKind(), task.eventId(), error);
        retryStore.dead(task.eventId(), task.retryKind(),
                RetryReason.PUBLISH_FAILED, null, error);
    }

    /**
     * Applies symmetric jitter to a delay.
     * <p>
     * The delay is multiplied by a random factor in
     * {@code [1 - jitter, 1 + jitter]}. A non-positive jitter leaves the delay
     * unchanged.
     *
     * @param delay  base delay
     * @param jitter jitter fraction, typically between 0 and 1
     * @return the jittered delay
     */
    public static Duration withJitter(Duration delay, double jitter) {
        Objects.requireNonNull(delay, "delay must not be null");
        if (jitter <= 0.0) {
            return delay;
        }
        long millis = delay.toMillis();
        long bound = Math.max(1L, (long) (millis * jitter));
        long offset = ThreadLocalRandom.current().nextLong(-bound, bound + 1);
        return Duration.ofMillis(Math.max(0L, millis + offset));
    }
}
