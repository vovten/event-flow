package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled cleanup of old terminal events from the {@link EventStore}.
 * <p>
 * Periodically deletes events with terminal statuses ({@link EventStatus#HANDLED},
 * {@link EventStatus#UNDEFINED}) that are older than a configured maximum age.
 * Deletion is performed in batches with a configurable pause between batches
 * to reduce database load and avoid long-running transactions.
 * <p>
 * {@link EventStatus#UNDEFINED} is included because it is the terminal status for
 * events published with {@code PERSISTED} lifecycle — they are saved once and never
 * transition to another status, making them safe to clean up after they age out.
 * <p>
 * {@link EventStatus#FAILED} events are never cleaned up automatically —
 * they are preserved for manual inspection and potential retry.
 * <p>
 * Implements {@link AutoCloseable} for resource management.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.2
 */
public final class EventCleanupScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EventCleanupScheduler.class);

    /**
     * Maximum events to delete in a single cleanup cycle.
     * Beyond this, leftover events are picked up by the next cycle.
     * Prevents a single cycle from running indefinitely on a backlog
     * of millions of events.
     */
    private static final int MAX_DELETE_PER_CYCLE = 100_000;

    /**
     * Terminal statuses safe for automatic cleanup.
     * HANDLED = successfully processed,
     * UNDEFINED = PERSISTED-lifecycle events that never transition further.
     */
    private static final List<EventStatus> TERMINAL_STATUSES = List.of(EventStatus.HANDLED, EventStatus.UNDEFINED);

    private final EventStore eventStore;
    private final Duration interval;
    private final Duration maxAge;
    private final int batchSize;
    private final Duration pauseBetweenBatches;
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a new EventCleanupScheduler.
     *
     * @param eventStore          the event store to clean up
     * @param interval            delay between cleanup cycles
     * @param maxAge              events older than this are eligible for deletion
     * @param batchSize           maximum number of events to delete per batch
     * @param pauseBetweenBatches pause between consecutive delete batches
     */
    public EventCleanupScheduler(EventStore eventStore,
                                  Duration interval,
                                  Duration maxAge,
                                  int batchSize,
                                  Duration pauseBetweenBatches) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.interval = Objects.requireNonNull(interval, "interval must not be null");
        this.maxAge = Objects.requireNonNull(maxAge, "maxAge must not be null");
        this.batchSize = batchSize;
        this.pauseBetweenBatches = Objects.requireNonNull(pauseBetweenBatches,
                "pauseBetweenBatches must not be null");
        if (!pauseBetweenBatches.isZero() && pauseBetweenBatches.compareTo(interval) >= 0) {
            log.warn("pauseBetweenBatches ({}) is >= cleanup interval ({}). "
                    + "The next cycle may be delayed while the previous batch pause is still active.",
                    pauseBetweenBatches, interval);
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "event-cleanup-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the cleanup scheduler. It will begin cleaning up old events
     * after the configured interval with jitter to avoid thundering herd
     * when multiple instances start simultaneously.
     */
    public void start() {
        long period = interval.toMillis();
        long initialDelay = period < 2 ? period : ThreadLocalRandom.current().nextLong(period);
        scheduler.scheduleWithFixedDelay(
                this::cleanupCycle,
                initialDelay,
                period,
                TimeUnit.MILLISECONDS
        );
        log.info("Event cleanup scheduler started: interval={}, maxAge={}, batchSize={}, pause={}",
                interval, maxAge, batchSize, pauseBetweenBatches);
    }

    /**
     * Stops the cleanup scheduler gracefully.
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
        log.info("Event cleanup scheduler stopped");
    }

    /**
     * Performs a single cleanup cycle: deletes old terminal events in batches
     * with pauses between batches.
     * <p>
     * Each batch runs in its own JDBC connection, so the pause genuinely reduces
     * database load. A single cycle deletes at most
     * {@value #MAX_DELETE_PER_CYCLE} events; the rest are picked up
     * by the next cycle.
     */
    public void cleanupCycle() {
        try {
            Instant deadline = Instant.now().minus(maxAge);
            int totalDeleted = deleteInBatches(deadline);
            logResult(totalDeleted);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Cleanup cycle interrupted");
        } catch (Exception e) {
            log.error("Cleanup cycle failed", e);
        }
    }

    private int deleteInBatches(Instant deadline) throws InterruptedException {
        int totalDeleted = 0;
        while (totalDeleted < MAX_DELETE_PER_CYCLE) {
            int remaining = MAX_DELETE_PER_CYCLE - totalDeleted;
            int currentBatchSize = Math.min(batchSize, remaining);
            int deleted = eventStore.deleteByStatuses(TERMINAL_STATUSES, deadline, currentBatchSize);
            if (deleted == 0) {
                break;
            }
            totalDeleted += deleted;
            pauseIfFullBatch(deleted, currentBatchSize);
            if (deleted < batchSize) {
                break;
            }
        }
        if (totalDeleted == MAX_DELETE_PER_CYCLE) {
            log.info("Cleanup cycle reached max per cycle ({}), deferring remaining events",
                    MAX_DELETE_PER_CYCLE);
        }
        return totalDeleted;
    }

    private void pauseIfFullBatch(int deleted, int batchSize) throws InterruptedException {
        if (deleted != batchSize || pauseBetweenBatches.isZero()) {
            return;
        }
        long pauseMs = Math.min(pauseBetweenBatches.toMillis(), interval.toMillis() / 2);
        if (pauseMs > 0) {
            Thread.sleep(pauseMs);
        }
    }

    private void logResult(int totalDeleted) {
        if (totalDeleted > 0) {
            log.info("Cleanup cycle: deleted {} events older than {}",
                    totalDeleted, formatDuration(maxAge));
        }
    }

    private String formatDuration(Duration d) {
        long days = d.toDays();
        long hours = d.toHours() % 24;
        long minutes = d.toMinutes() % 60;
        long seconds = d.toSeconds() % 60;
        StringBuilder sb = new StringBuilder();
        if (days > 0) sb.append(days).append("d ");
        if (hours > 0) sb.append(hours).append("h ");
        if (minutes > 0) sb.append(minutes).append("m ");
        if (seconds > 0) sb.append(seconds).append("s");
        if (!sb.isEmpty()) {
            return sb.toString().strip();
        }
        return d.toMillis() + " ms";
    }

    @Override
    public void close() {
        stop();
    }
}
