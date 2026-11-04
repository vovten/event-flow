package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.retry.RetryKind;
import io.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled durable retries for the {@link RetryKind#PROCESSING} domain.
 * <p>
 * The scheduler reads due {@code PROCESSING} rows, claims them optimistically
 * and re-publishes the stored event with the {@code PROCESSING} retry context.
 * Durable processing retries re-run all handlers, so handlers must be
 * idempotent.
 * <p>
 * The row stays {@link RetryState#IN_PROGRESS} until a {@code SuccessAck} or
 * {@code FailureAck} is routed to the {@link LifecycleRepository}. If the lease
 * expires, the scheduler returns the row to {@link RetryState#PENDING} with the
 * next attempt and a jittered exponential backoff. When
 * {@link ProcessingRetryOptions#maxRetries()} is exhausted, the row and the
 * event are dead-lettered.
 * <p>
 * Attempt numbers are 1-based and count the original publish. The row stores
 * the last attempted number; each scheduler publish uses {@code attempt + 1}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class ProcessingRetryScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProcessingRetryScheduler.class);

    private final RetrySchedulerSupport support;
    private final EventStore eventStore;
    private final EventPublisher publisher;
    private final ProcessingRetryOptions options;
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a processing retry scheduler with its own support object.
     *
     * @param retryStore          durable retry store
     * @param eventStore          event store holding the payloads
     * @param lifecycleRepository transactional lifecycle facade for dead-lettering
     * @param publisher           publisher used to re-publish stored events
     * @param options             scheduler options
     */
    public ProcessingRetryScheduler(EventRetryStore retryStore,
                                    EventStore eventStore,
                                    LifecycleRepository lifecycleRepository,
                                    EventPublisher publisher,
                                    ProcessingRetryOptions options) {
        this(new RetrySchedulerSupport(retryStore, lifecycleRepository,
                        "processing-retry-" + UUID.randomUUID()),
                eventStore, publisher, options);
    }

    /**
     * Creates a processing retry scheduler with a shared support object.
     *
     * @param support    shared scheduler support
     * @param eventStore event store holding the payloads
     * @param publisher  publisher used to re-publish stored events
     * @param options    scheduler options
     */
    public ProcessingRetryScheduler(RetrySchedulerSupport support,
                                    EventStore eventStore,
                                    EventPublisher publisher,
                                    ProcessingRetryOptions options) {
        this.support = Objects.requireNonNull(support, "support must not be null");
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "processing-retry-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts periodic retry cycles.
     */
    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::retryCycle,
                options.interval().toMillis(),
                options.interval().toMillis(),
                TimeUnit.MILLISECONDS
        );
        log.info("Processing retry scheduler started: interval={}, ackTimeout={}, maxRetries={}, batchSize={}",
                options.interval(), options.ackTimeout(), options.maxRetries(), options.batchSize());
    }

    /**
     * Stops the scheduler gracefully.
     */
    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("Processing retry scheduler stopped");
    }

    /**
     * Performs a single processing retry cycle.
     * <p>
     * Due pending rows are re-published. Expired in-progress rows are
     * rescheduled or dead-lettered. One failing event does not stop the cycle.
     */
    void retryCycle() {
        Instant now = Instant.now();
        List<RetryTask> due = support.findDue(
                RetryKind.PROCESSING, options.service(), now, options.batchSize());
        if (due.isEmpty()) {
            log.trace("No processing retries are due");
            return;
        }
        for (RetryTask task : due) {
            try {
                process(task, now);
            } catch (RuntimeException e) {
                log.error("Failed to process processing retry for event {}", task.eventId(), e);
            }
        }
    }

    private void process(RetryTask task, Instant now) {
        if (task.state() == RetryState.IN_PROGRESS) {
            if (task.isClaimExpired(now)) {
                handleExpiredClaim(task, now);
            } else {
                log.trace("Processing retry for event {} is still in progress", task.eventId());
            }
            return;
        }
        if (task.state() == RetryState.PENDING && task.isDue(now)) {
            deliver(task, now);
        }
    }

    private void deliver(RetryTask task, Instant now) {
        int nextAttempt = task.attempt() + 1;
        if (isBudgetExhausted(nextAttempt)) {
            support.deadLetter(task, RetryReason.CAP_EXCEEDED, "processing retry budget exhausted");
            return;
        }
        if (!support.claimNextAttempt(task, nextAttempt, now, options.ackTimeout())) {
            log.debug("Processing retry for event {} was already claimed", task.eventId());
            return;
        }
        Optional<StoredEvent> stored = eventStore.findById(task.eventId());
        if (stored.isEmpty()) {
            support.markDeadWithoutEvent(task, "stored event not found");
            return;
        }
        publish(task, stored.get(), nextAttempt, now);
    }

    private void publish(RetryTask task, StoredEvent stored, int attempt, Instant now) {
        try {
            Event event = RetryEventFactory.forRetry(stored, RetryKind.PROCESSING, attempt);
            SendResults results = publisher.publish(event).join();
            if (results == null || results.isAllFailure()) {
                String error = results == null ? "null publish result" : results.getSummary();
                scheduleNextAttempt(task, attempt, now, RetryReason.PUBLISH_FAILED, null, error);
                return;
            }
            log.info("Re-published processing retry for event {} (attempt {}/{}); awaiting final ack",
                    task.eventId(), attempt, maxAttempts());
        } catch (RuntimeException e) {
            log.warn("Processing retry publish failed for event {} (attempt {}): {}",
                    task.eventId(), attempt, e.getMessage());
            scheduleNextAttempt(task, attempt, now, RetryReason.PUBLISH_FAILED,
                    e.getClass().getName(), errorDetails(e));
        }
    }

    private void handleExpiredClaim(RetryTask task, Instant now) {
        if (!support.claim(task, now, options.ackTimeout())) {
            log.debug("Expired processing retry for event {} was claimed by another instance",
                    task.eventId());
            return;
        }
        log.warn("Processing ack timeout for event {} (attempt {}/{})",
                task.eventId(), task.attempt(), maxAttempts());
        scheduleNextAttempt(task, task.attempt(), now, RetryReason.PROCESSING_TIMEOUT, null,
                "no final ack within " + options.ackTimeout());
    }

    private void scheduleNextAttempt(RetryTask task, int performedAttempt, Instant now,
                                     RetryReason reason, String errorType, String error) {
        int nextAttempt = performedAttempt + 1;
        if (isBudgetExhausted(nextAttempt)) {
            support.deadLetter(task, RetryReason.CAP_EXCEEDED, error);
            return;
        }
        Duration delay = RetrySchedulerSupport.withJitter(
                options.backoff(performedAttempt), options.jitter());
        support.reschedule(task, nextAttempt, reason, now.plus(delay), errorType, error);
        log.debug("Scheduled processing retry for event {}: attempt {}/{}, reason={}, delay={}",
                task.eventId(), nextAttempt, maxAttempts(), reason, delay);
    }

    private boolean isBudgetExhausted(int attempt) {
        return attempt > maxAttempts();
    }

    private int maxAttempts() {
        return options.maxRetries() + 1;
    }

    private static String errorDetails(Throwable error) {
        return error.getMessage() != null ? error.getMessage() : error.getClass().getName();
    }

    @Override
    public void close() {
        stop();
    }
}
