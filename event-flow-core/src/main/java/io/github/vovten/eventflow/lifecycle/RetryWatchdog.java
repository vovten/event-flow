package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.retry.RetryKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Watchdog that creates or raises durable retries for missing acknowledgments.
 * <p>
 * It scans {@code event_store} for events that are older than the configured
 * ack timeouts:
 * <ul>
 *   <li>{@link EventStatus#PUBLISHED} older than the delivery timeout raises a
 *       {@link RetryKind#DELIVERY} row;</li>
 *   <li>{@link EventStatus#ACCEPTED} older than the processing timeout raises a
 *       {@link RetryKind#PROCESSING} row, only when processing retries are
 *       enabled.</li>
 * </ul>
 * Existing {@code IN_PROGRESS} and {@code DEAD} rows are left untouched.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetryWatchdog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RetryWatchdog.class);

    private final EventStore eventStore;
    private final EventRetryStore retryStore;
    private final RetryWatchdogOptions options;
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a retry watchdog.
     *
     * @param eventStore event store to scan
     * @param retryStore durable retry store to update
     * @param options    watchdog options
     */
    public RetryWatchdog(EventStore eventStore,
                         EventRetryStore retryStore,
                         RetryWatchdogOptions options) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.retryStore = Objects.requireNonNull(retryStore, "retryStore must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "retry-watchdog");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts periodic watchdog cycles.
     */
    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::watchdogCycle,
                options.interval().toMillis(),
                options.interval().toMillis(),
                TimeUnit.MILLISECONDS
        );
        log.info("Retry watchdog started: interval={}, deliveryAckTimeout={}, processingAckTimeout={}, "
                        + "processingEnabled={}",
                options.interval(), options.deliveryAckTimeout(),
                options.processingAckTimeout(), options.processingEnabled());
    }

    /**
     * Stops the watchdog gracefully.
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
        log.info("Retry watchdog stopped");
    }

    /**
     * Performs a single watchdog cycle.
     */
    void watchdogCycle() {
        Instant now = Instant.now();
        scan(EventStatus.PUBLISHED, RetryKind.DELIVERY, RetryReason.ACK_TIMEOUT,
                now.minus(options.deliveryAckTimeout()), now);
        if (options.processingEnabled()) {
            scan(EventStatus.ACCEPTED, RetryKind.PROCESSING, RetryReason.PROCESSING_TIMEOUT,
                    now.minus(options.processingAckTimeout()), now);
        }
    }

    private void scan(EventStatus status, RetryKind retryKind, RetryReason reason,
                      Instant before, Instant now) {
        List<StoredEvent> stale = eventStore.findByStatuses(
                List.of(status), before, options.batchSize());
        for (StoredEvent event : stale) {
            if (!options.service().equals(event.service())) {
                continue;
            }
            try {
                raise(event, retryKind, reason, now);
            } catch (RuntimeException e) {
                log.error("Failed to raise {} retry for stale event {}",
                        retryKind, event.eventId(), e);
            }
        }
    }

    private void raise(StoredEvent event, RetryKind retryKind, RetryReason reason, Instant now) {
        Optional<RetryTask> existing = retryStore.find(event.eventId(), retryKind);
        if (existing.isEmpty()) {
            int attempt = Math.max(1, event.retryCount() + 1);
            retryStore.schedule(RetryTask.pending(
                    event.eventId(), options.service(), retryKind, attempt, reason, now, null));
            log.warn("Created {} retry for stale event {} (status {})",
                    retryKind, event.eventId(), event.status());
            return;
        }
        RetryTask task = existing.get();
        if (task.state() == RetryState.PENDING && task.nextAttemptAt().isAfter(now)) {
            retryStore.schedule(task.withNextAttempt(
                    task.attempt(), reason, now, task.lastErrorType(), task.lastError()));
            log.warn("Raised {} retry for stale event {} (status {})",
                    retryKind, event.eventId(), event.status());
        } else {
            log.trace("{} retry for stale event {} is already {}", retryKind, event.eventId(),
                    task.state());
        }
    }

    @Override
    public void close() {
        stop();
    }
}
