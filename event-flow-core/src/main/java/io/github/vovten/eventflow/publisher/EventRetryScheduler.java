package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.store.EventStatus;
import io.github.vovten.eventflow.store.EventStore;
import io.github.vovten.eventflow.store.StoredEvent;
import io.github.vovten.eventflow.util.EventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled retry mechanism for failed events in the {@link EventStore}.
 * <p>
 * Periodically scans for events with status {@link EventStatus#PUBLISH_FAILED}
 * or {@link EventStatus#HANDLE_FAILED} that are older than a configured minimum age
 * and have not exceeded the maximum retry count. Eligible events are:
 * <ol>
 *   <li>Retry count incremented (via {@link EventStore#updateStatus})</li>
 *   <li>Status reset to {@link EventStatus#NEW}</li>
 *   <li>Event deserialized from JSON and re-published</li>
 * </ol>
 * <p>
 * The delay between retry attempts increases exponentially:
 * {@code minAge × 2^retryCount}. The first retry waits {@code minAge},
 * the second waits {@code 2 × minAge}, the third {@code 4 × minAge}, and so on.
 * <p>
 * Implements {@link AutoCloseable} for resource management.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public final class EventRetryScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EventRetryScheduler.class);

    private final EventStore eventStore;
    private final EventPublisher publisher;
    private final Duration interval;
    private final Duration minAge;
    private final int maxRetries;
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a new EventRetryScheduler.
     *
     * @param eventStore the event store to scan for failed events
     * @param publisher  the publisher for re-publishing failed events
     * @param interval   delay between retry cycles
     * @param minAge     base backoff interval for retries (delay = minAge × 2^retryCount)
     * @param maxRetries maximum number of retry attempts before giving up
     */
    public EventRetryScheduler(EventStore eventStore,
                               EventPublisher publisher,
                               Duration interval,
                               Duration minAge,
                               int maxRetries) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.interval = Objects.requireNonNull(interval, "interval must not be null");
        this.minAge = Objects.requireNonNull(minAge, "minAge must not be null");
        this.maxRetries = maxRetries;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "event-retry-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the retry scheduler. It will begin scanning for failed events
     * after the configured interval.
     */
    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::retryCycle,
                interval.toMillis(),
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
        log.info("Event retry scheduler started: interval={}, minAge={}, maxRetries={}",
                interval, minAge, maxRetries);
    }

    /**
     * Stops the retry scheduler gracefully.
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
        log.info("Event retry scheduler stopped");
    }

    /**
     * Performs a single retry cycle: scans for failed events and retries eligible ones.
     */
    void retryCycle() {
        try {
            Instant deadline = Instant.now().minus(minAge);
            List<StoredEvent> failedEvents = eventStore.findByStatus(EventStatus.PUBLISH_FAILED, deadline);
            failedEvents.addAll(eventStore.findByStatus(EventStatus.HANDLE_FAILED, deadline));

            if (failedEvents.isEmpty()) {
                log.trace("No failed events to retry");
                return;
            }
            log.debug("Found {} failed events eligible for retry", failedEvents.size());

            for (StoredEvent event : failedEvents) {
                if (event.retryCount() >= maxRetries) {
                    log.warn("Event {} exceeded max retries ({}/{}), skipping",
                            event.eventId(), event.retryCount(), maxRetries);
                    continue;
                }
                if (!isBackoffElapsed(event)) {
                    continue;
                }
                retryEvent(event);
            }
        } catch (Exception e) {
            log.error("Error during retry cycle", e);
        }
    }

    private void retryEvent(StoredEvent stored) {
        try {
            Event event = EventUtils.fromJson(stored.payload(), Event.class);
            eventStore.updateStatus(stored.eventId(), EventStatus.NEW, null);
            publisher.publish(event);
            log.info("Retried event id={} (attempt {}/{})", stored.eventId(), stored.retryCount() + 1, maxRetries);
        } catch (Exception e) {
            log.error("Failed to retry event id={}: {}", stored.eventId(), e.getMessage());
        }
    }

    /**
     * Checks whether the exponential backoff period has elapsed for this event.
     * <p>
     * The delay grows with each attempt: {@code minAge × 2^retryCount}.
     * The timer starts from the event's {@link StoredEvent#updatedAt()} timestamp,
     * which is refreshed after each failed attempt.
     *
     * @param event the failed event
     * @return true if enough time has passed to retry
     */
    private boolean isBackoffElapsed(StoredEvent event) {
        Duration backoff = minAge.multipliedBy(1L << event.retryCount());
        Instant retryAt = event.updatedAt().plus(backoff);
        boolean elapsed = !Instant.now().isBefore(retryAt);
        if (!elapsed) {
            log.trace("Backoff not elapsed for event {}, retry #{}, retryAt={}",
                    event.eventId(), event.retryCount() + 1, retryAt);
        }
        return elapsed;
    }

    @Override
    public void close() {
        stop();
    }
}
