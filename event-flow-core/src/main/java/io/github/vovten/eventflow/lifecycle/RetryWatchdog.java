package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
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
 * Watchdog that creates durable retries for missing acknowledgments.
 * <p>
 * It scans {@code event_store} for events owned by its service that are older
 * than the configured ack timeouts:
 * <ul>
 *   <li>{@link EventStatus#PUBLISHED} older than the delivery timeout creates a
 *       {@link RetryKind#DELIVERY} row;</li>
 *   <li>{@link EventStatus#ACCEPTED} older than the processing timeout creates a
 *       {@link RetryKind#PROCESSING} row, only when processing retries are
 *       enabled.</li>
 * </ul>
 * Existing rows are never modified: an {@code IN_PROGRESS} or {@code DEAD} row
 * is left as is, and a {@code PENDING} row keeps its scheduled
 * {@code nextAttemptAt}. In particular, the watchdog does not pull a
 * backoff-delayed row forward to {@code now}, so a consumer-provided retry
 * delay is preserved.
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
                        + "deliveryEnabled={}, processingEnabled={}",
                options.interval(), options.deliveryAckTimeout(),
                options.processingAckTimeout(), options.deliveryEnabled(),
                options.processingEnabled());
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
        if (options.deliveryEnabled()) {
            scan(EventStatus.PUBLISHED, RetryKind.DELIVERY, RetryReason.ACK_TIMEOUT,
                    now.minus(options.deliveryAckTimeout()), now);
        }
        if (options.processingEnabled()) {
            scan(EventStatus.ACCEPTED, RetryKind.PROCESSING, RetryReason.PROCESSING_TIMEOUT,
                    now.minus(options.processingAckTimeout()), now);
        }
    }

    private void scan(EventStatus status, RetryKind retryKind, RetryReason reason,
                      Instant before, Instant now) {
        List<StoredEvent> stale = eventStore.findByStatusesAndService(
                List.of(status), before, options.batchSize(), options.service());
        for (StoredEvent event : stale) {
            try {
                ensureRetry(event, retryKind, reason, now);
            } catch (RuntimeException e) {
                log.error("Failed to ensure {} retry for stale event {}",
                        retryKind, event.eventId(), e);
            }
        }
    }

    private void ensureRetry(StoredEvent event, RetryKind retryKind, RetryReason reason, Instant now) {
        Optional<RetryTask> existing = retryStore.find(event.eventId(), retryKind);
        if (existing.isPresent()) {
            log.trace("{} retry for stale event {} is already {}",
                    retryKind, event.eventId(), existing.get().state());
            return;
        }
        // retry_count is a cross-domain legacy counter, not this kind's
        // attempt number. A watchdog-created row is the first attempt of
        // its own DELIVERY/PROCESSING domain.
        retryStore.schedule(RetryTask.pending(
                event.eventId(), options.service(), retryKind, 1, reason, now, null));
        log.warn("Created {} retry for stale event {} (status {})",
                retryKind, event.eventId(), event.status());
    }

    @Override
    public void close() {
        stop();
    }
}
