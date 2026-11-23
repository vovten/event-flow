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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled durable retries for the {@link RetryKind#DELIVERY} domain.
 * <p>
 * The scheduler reads due {@code DELIVERY} rows from the retry store, claims
 * them optimistically and re-publishes the stored event with the
 * {@code DELIVERY} retry context. The row stays {@link RetryState#IN_PROGRESS}
 * until an {@code AcceptedAck} removes it. If the lease expires first, the row
 * is returned to {@link RetryState#PENDING} with an incremented attempt. When
 * {@link DeliveryRetryOptions#maxRetries()} is exhausted, the row and the event
 * are dead-lettered through the {@link LifecycleRepository}.
 * <p>
 * The scheduler only selects {@code DELIVERY} rows. It never scans
 * {@code event_store} by status, so
 * {@link io.github.vovten.eventflow.lifecycle.store.EventStatus#ACCEPTED} events
 * are not retried by the delivery layer.
 * <p>
 * When a processing retry is also pending or in progress for the same event,
 * delivery defers to it: a processing re-publish is itself a delivery, so this
 * avoids a duplicate publish. Attempt numbers are 1-based and count the
 * original publish; each scheduler publish uses {@code attempt + 1}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class DeliveryRetryScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeliveryRetryScheduler.class);

    private final RetrySchedulerSupport support;
    private final EventStore eventStore;
    private final EventPublisher publisher;
    private final DeliveryRetryOptions options;
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a delivery retry scheduler with its own support object.
     *
     * @param retryStore          durable retry store
     * @param eventStore          event store holding the payloads
     * @param lifecycleRepository transactional lifecycle facade for dead-lettering
     * @param publisher           publisher used to re-publish stored events
     * @param options             scheduler options
     */
    public DeliveryRetryScheduler(EventRetryStore retryStore,
                                  EventStore eventStore,
                                  LifecycleRepository lifecycleRepository,
                                  EventPublisher publisher,
                                  DeliveryRetryOptions options) {
        this(new RetrySchedulerSupport(retryStore, lifecycleRepository,
                        "delivery-retry-" + UUID.randomUUID()),
                eventStore, publisher, options);
    }

    /**
     * Creates a delivery retry scheduler with a shared support object.
     *
     * @param support   shared scheduler support
     * @param eventStore event store holding the payloads
     * @param publisher  publisher used to re-publish stored events
     * @param options    scheduler options
     */
    public DeliveryRetryScheduler(RetrySchedulerSupport support,
                                  EventStore eventStore,
                                  EventPublisher publisher,
                                  DeliveryRetryOptions options) {
        this.support = Objects.requireNonNull(support, "support must not be null");
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "delivery-retry-scheduler");
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
        log.info("Delivery retry scheduler started: interval={}, ackTimeout={}, maxRetries={}, batchSize={}",
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
        log.info("Delivery retry scheduler stopped");
    }

    /**
     * Performs a single delivery retry cycle.
     * <p>
     * Due pending rows are re-published. Expired in-progress rows are
     * rescheduled or dead-lettered. One failing event does not stop the cycle.
     */
    void retryCycle() {
        Instant now = Instant.now();
        List<RetryTask> due = support.findDue(RetryKind.DELIVERY, options.service(), now, options.batchSize());
        if (due.isEmpty()) {
            log.trace("No delivery retries are due");
            return;
        }
        List<CompletableFuture<Void>> inFlight = new ArrayList<>(due.size());
        for (RetryTask task : due) {
            try {
                inFlight.add(process(task, now));
            } catch (RuntimeException e) {
                log.error("Failed to process delivery retry for event {}", task.eventId(), e);
            }
        }
        // Publishes overlap; wait once for the batch so cycles do not pile up.
        // get() keeps the wait interruptible so stop() can still shut down.
        try {
            CompletableFuture.allOf(inFlight.toArray(CompletableFuture[]::new)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted while waiting for the delivery retry batch");
        } catch (ExecutionException e) {
            log.error("Delivery retry batch failed", e.getCause());
        }
    }

    private CompletableFuture<Void> process(RetryTask task, Instant now) {
        if (task.state() == RetryState.IN_PROGRESS) {
            if (task.isClaimExpired(now)) {
                return handleExpiredClaim(task, now);
            }
            log.trace("Delivery retry for event {} is still in progress", task.eventId());
            return CompletableFuture.completedFuture(null);
        }
        if (task.state() == RetryState.PENDING && task.isDue(now)) {
            return deliver(task, now);
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> deliver(RetryTask task, Instant now) {
        if (processingOwnsDelivery(task)) {
            log.debug("Deferring delivery retry for event {}: a processing retry is active", task.eventId());
            return CompletableFuture.completedFuture(null);
        }
        int nextAttempt = task.attempt() + 1;
        if (isBudgetExhausted(nextAttempt)) {
            support.deadLetter(task, RetryReason.CAP_EXCEEDED, "delivery retry budget exhausted");
            return CompletableFuture.completedFuture(null);
        }
        if (!support.claimNextAttempt(task, nextAttempt, now, options.ackTimeout())) {
            log.debug("Delivery retry for event {} was already claimed", task.eventId());
            return CompletableFuture.completedFuture(null);
        }
        Optional<StoredEvent> stored = eventStore.findById(task.eventId());
        if (stored.isEmpty()) {
            support.markDeadWithoutEvent(task, "stored event not found");
            return CompletableFuture.completedFuture(null);
        }
        return publish(task, stored.get(), nextAttempt);
    }

    private boolean processingOwnsDelivery(RetryTask task) {
        return support.find(task.eventId(), RetryKind.PROCESSING)
                .filter(row -> row.state() == RetryState.PENDING
                        || row.state() == RetryState.IN_PROGRESS)
                .isPresent();
    }

    private CompletableFuture<Void> publish(RetryTask task, StoredEvent stored, int attempt) {
        Optional<Event> event = createRetryEvent(task, stored, attempt);
        if (event.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return submit(task, event.get(), attempt);
    }

    private Optional<Event> createRetryEvent(RetryTask task, StoredEvent stored, int attempt) {
        try {
            return Optional.of(RetryEventFactory.forRetry(stored, RetryKind.DELIVERY, attempt));
        } catch (RuntimeException | LinkageError e) {
            handleUnreadablePayload(task, attempt, e);
            return Optional.empty();
        }
    }

    private void handleUnreadablePayload(RetryTask task, int attempt, Throwable error) {
        String details = PayloadFailure.describe(error);
        if (PayloadFailure.isTransient(error)
                && attempt <= options.payloadRetryThreshold()
                && !isBudgetExhausted(attempt + 1)) {
            log.warn("Transient payload failure for delivery retry of event {} (attempt {}); retrying: {}",
                    task.eventId(), attempt, details);
            scheduleNextAttempt(task, attempt, Instant.now(), RetryReason.PAYLOAD_UNREADABLE,
                    error.getClass().getName(), details);
            return;
        }
        log.error("Unreadable payload for delivery retry of event {} (attempt {}); dead-lettering: {}",
                task.eventId(), attempt, details);
        support.deadLetter(task, RetryReason.PAYLOAD_UNREADABLE, details);
    }

    private CompletableFuture<Void> submit(RetryTask task, Event event, int attempt) {
        CompletableFuture<SendResults> published;
        try {
            published = publisher.publish(event);
        } catch (RuntimeException e) {
            handlePublishError(task, attempt, e);
            return CompletableFuture.completedFuture(null);
        }
        return published.handle((results, error) -> {
            if (error != null) {
                handlePublishError(task, attempt, unwrapCompletion(error));
            } else if (results == null || results.isAllFailure()) {
                handleFailedResults(task, attempt, results);
            } else {
                log.info("Re-published delivery retry for event {} (attempt {}/{})",
                        task.eventId(), attempt, maxAttempts());
            }
            return null;
        });
    }

    private void handlePublishError(RetryTask task, int attempt, Throwable error) {
        handlePublishFailure(task, attempt, error.getClass().getName(), errorDetails(error));
    }

    private void handleFailedResults(RetryTask task, int attempt, SendResults results) {
        String failure = results == null ? "null publish result" : results.getSummary();
        handlePublishFailure(task, attempt, null, failure);
    }

    private void handlePublishFailure(RetryTask task, int attempt, String errorType, String error) {
        log.warn("Delivery retry publish failed for event {} (attempt {}): {}", task.eventId(), attempt, error);
        scheduleNextAttempt(task, attempt, Instant.now(), RetryReason.PUBLISH_FAILED, errorType, error);
    }

    private static Throwable unwrapCompletion(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    private CompletableFuture<Void> handleExpiredClaim(RetryTask task, Instant now) {
        if (!support.claim(task, now, options.ackTimeout())) {
            log.debug("Expired delivery retry for event {} was claimed by another instance", task.eventId());
            return CompletableFuture.completedFuture(null);
        }
        log.warn("Delivery ack timeout for event {} (attempt {}/{})", task.eventId(), task.attempt(), maxAttempts());
        scheduleNextAttempt(task, task.attempt(), now, RetryReason.ACK_TIMEOUT, null,
                "no AcceptedAck within " + options.ackTimeout());
        return CompletableFuture.completedFuture(null);
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
        log.debug("Scheduled delivery retry for event {}: attempt {}/{}, reason={}, delay={}",
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
