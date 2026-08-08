package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
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
 * Scheduled retry mechanism for failed, stuck, and orphaned events in the {@link EventStore}.
 * <p>
 * Periodically scans for events with status {@link EventStatus#FAILED},
 * {@link EventStatus#PUBLISHED}, or {@link EventStatus#NEW}
 * that are older than a configured minimum age
 * and have not exceeded the maximum retry count. Eligible events are:
 * <ol>
 *   <li>Deserialized from JSON payload</li>
 *   <li>Re-published via the configured {@link EventPublisher}</li>
 *   <li>Status reset to {@link EventStatus#NEW} and retry count incremented
 *       by the {@link io.github.vovten.eventflow.lifecycle.EventLifecyclePublisher}</li>
 * </ol>
 * <p>
 * {@link EventStatus#PUBLISHED} events are retried to handle cases where
 * the acknowledgment event (ack) was lost and the event is stuck in the
 * published state. {@link EventStatus#NEW} events are retried to handle
 * cases where the application crashed before the initial publish completed.
 * The same backoff and retry limit apply for all statuses.
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
    private final int batchSize;
    private final String service;
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a new EventRetryScheduler that only retries events attributed to
     * the given service name.
     * <p>
     * When the event store is shared between multiple services, the scheduler
     * must only re-publish events it originally published itself. The service
     * name is therefore required — retrying events from all services is not
     * supported.
     *
     * @param eventStore the event store to scan for failed events
     * @param publisher  the publisher for re-publishing failed events
     * @param interval   delay between retry cycles
     * @param minAge     base backoff interval for retries (delay = minAge × 2^retryCount)
     * @param maxRetries maximum number of retry attempts before giving up
     * @param batchSize  maximum number of events to fetch per retry cycle
     * @param service    the local service name to filter retry candidates by
     *                   (must not be null or blank)
     * @throws IllegalArgumentException if service is null or blank
     */
    public EventRetryScheduler(EventStore eventStore,
                                EventPublisher publisher,
                                Duration interval,
                                Duration minAge,
                                int maxRetries,
                                int batchSize,
                                String service) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.interval = Objects.requireNonNull(interval, "interval must not be null");
        this.minAge = Objects.requireNonNull(minAge, "minAge must not be null");
        this.maxRetries = maxRetries;
        this.batchSize = batchSize;
        if (service == null || service.isBlank()) {
            throw new IllegalArgumentException("service must not be null or blank");
        }
        this.service = service;
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
        log.info("Event retry scheduler started: interval={}, minAge={}, maxRetries={}, batchSize={} (FAILED, PUBLISHED, NEW)",
                interval, minAge, maxRetries, batchSize);
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
     * Performs a single retry cycle: scans for events eligible for retry
     * (FAILED, PUBLISHED, NEW, or manually marked with {@code retry = true})
     * and retries them.
     * <p>
     * Events manually marked for retry ({@code retry = true}) are retried
     * regardless of their current status and bypass the maxRetries and backoff
     * checks. Other events are subject to the configured retry limits.
     * <p>
     * NEW events are included to handle cases where the event was persisted
     * but the application crashed before the publish completed. These events
     * would otherwise be stuck in the NEW state indefinitely.
     */
    void retryCycle() {
        try {
            Instant deadline = Instant.now().minus(minAge);
            List<EventStatus> statuses = List.of(EventStatus.FAILED, EventStatus.PUBLISHED, EventStatus.NEW);
            List<StoredEvent> events = eventStore.findRetryableEvents(statuses, deadline, batchSize, service);
            if (events.isEmpty()) {
                log.trace("No events to retry");
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Found {} events eligible for retry ({} with retry flag)",
                        events.size(), events.stream().filter(StoredEvent::retry).count());
            }
            for (StoredEvent event : events) {
                if (event.retry()) {
                    log.info("Manual retry for event id={}", event.eventId());
                    retryEvent(event);
                } else {
                    retryIfEligible(event);
                }
            }
        } catch (Exception e) {
            log.error("Error during retry cycle", e);
        }
    }

    private void retryIfEligible(StoredEvent event) {
        if (event.retryCount() >= maxRetries) {
            log.warn("Event {} exceeded max retries ({}/{}), skipping",
                    event.eventId(), event.retryCount(), maxRetries);
            return;
        }
        if (!event.isReadyForRetry(minAge)) {
            if (log.isTraceEnabled()) {
                log.trace("Backoff not elapsed for event {}, retry #{}, retryAt={}",
                        event.eventId(), event.retryCount() + 1, computeRetryAt(event));
            }
            return;
        }
        retryEvent(event);
    }

    /**
     * Computes the earliest retry time for an event based on exponential backoff.
     *
     * @param event the event to check
     * @return the instant when this event becomes eligible for retry
     */
    private Instant computeRetryAt(StoredEvent event) {
        Duration backoff = minAge.multipliedBy(1L << event.retryCount());
        return event.updatedAt().plus(backoff);
    }

    private void retryEvent(StoredEvent stored) {
        try {
            Event event = EventUtils.fromJson(stored.payload(), Event.class);
            publisher.publish(event);
            log.info("Retried event id={} (attempt {}/{})", stored.eventId(), stored.retryCount() + 1, maxRetries);
        } catch (Exception e) {
            log.error("Failed to retry event id={}: {}", stored.eventId(), e.getMessage());
        }
    }

    @Override
    public void close() {
        stop();
    }
}
