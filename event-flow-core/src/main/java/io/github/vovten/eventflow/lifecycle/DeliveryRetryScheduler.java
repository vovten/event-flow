package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.retry.RetryKind;
import io.github.vovten.eventflow.retry.RetryMetadata;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.util.EventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled durable retries for the {@link RetryKind#DELIVERY} domain.
 * <p>
 * The scheduler reads due {@code DELIVERY} rows from the {@link EventRetryStore},
 * claims them optimistically and re-publishes the stored event with the
 * {@code DELIVERY} retry context. The row stays {@link RetryState#IN_PROGRESS}
 * until an {@code AcceptedAck} removes it. If the lease expires first, the row
 * is returned to {@link RetryState#PENDING} with an incremented attempt. When
 * {@link DeliveryRetryOptions#maxRetries()} is exhausted, the row and the event
 * are dead-lettered through the {@link LifecycleRepository}.
 * <p>
 * The scheduler only selects {@code DELIVERY} rows. It never scans
 * {@code event_store} by status, so {@link io.github.vovten.eventflow.lifecycle.store.EventStatus#ACCEPTED}
 * events are not retried by the delivery layer.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class DeliveryRetryScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeliveryRetryScheduler.class);

    private final EventRetryStore retryStore;
    private final EventStore eventStore;
    private final LifecycleRepository lifecycleRepository;
    private final EventPublisher publisher;
    private final DeliveryRetryOptions options;
    private final String owner;
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a delivery retry scheduler.
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
        this.retryStore = Objects.requireNonNull(retryStore, "retryStore must not be null");
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.lifecycleRepository = Objects.requireNonNull(lifecycleRepository,
                "lifecycleRepository must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.owner = "delivery-retry-" + UUID.randomUUID();
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
        List<RetryTask> due = retryStore.findDue(
                RetryKind.DELIVERY, options.service(), now, options.batchSize());
        if (due.isEmpty()) {
            log.trace("No delivery retries are due");
            return;
        }
        for (RetryTask task : due) {
            try {
                process(task, now);
            } catch (RuntimeException e) {
                log.error("Failed to process delivery retry for event {}", task.eventId(), e);
            }
        }
    }

    private void process(RetryTask task, Instant now) {
        if (task.state() == RetryState.IN_PROGRESS) {
            if (task.isClaimExpired(now)) {
                handleExpiredClaim(task, now);
            } else {
                log.trace("Delivery retry for event {} is still in progress", task.eventId());
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
            deadLetter(task, RetryReason.CAP_EXCEEDED, "delivery retry budget exhausted");
            return;
        }
        if (!claimNextAttempt(task, nextAttempt, now)) {
            log.debug("Delivery retry for event {} was already claimed", task.eventId());
            return;
        }
        Optional<StoredEvent> stored = eventStore.findById(task.eventId());
        if (stored.isEmpty()) {
            markDeadWithoutEvent(task, "stored event not found");
            return;
        }
        publish(task, stored.get(), nextAttempt, now);
    }

    private boolean claimNextAttempt(RetryTask task, int nextAttempt, Instant now) {
        retryStore.schedule(task.withNextAttempt(nextAttempt, task.reason(), now, null, null));
        return retryStore.claim(task.eventId(), RetryKind.DELIVERY, owner,
                now.plus(options.ackTimeout()));
    }

    private void publish(RetryTask task, StoredEvent stored, int attempt, Instant now) {
        try {
            Event event = toRetryEvent(stored, attempt);
            SendResults results = publisher.publish(event).join();
            if (results == null || results.isAllFailure()) {
                String error = results == null ? "null publish result" : results.getSummary();
                scheduleNextAttempt(task, attempt, now, RetryReason.PUBLISH_FAILED, null, error);
                return;
            }
            log.info("Re-published delivery retry for event {} (attempt {}/{})",
                    task.eventId(), attempt, maxAttempts());
        } catch (RuntimeException e) {
            log.warn("Delivery retry publish failed for event {} (attempt {}): {}",
                    task.eventId(), attempt, e.getMessage());
            scheduleNextAttempt(task, attempt, now, RetryReason.PUBLISH_FAILED,
                    e.getClass().getName(), errorDetails(e));
        }
    }

    private void handleExpiredClaim(RetryTask task, Instant now) {
        if (!retryStore.claim(task.eventId(), RetryKind.DELIVERY, owner,
                now.plus(options.ackTimeout()))) {
            log.debug("Expired delivery retry for event {} was claimed by another instance",
                    task.eventId());
            return;
        }
        log.warn("Delivery ack timeout for event {} (attempt {}/{})",
                task.eventId(), task.attempt(), maxAttempts());
        scheduleNextAttempt(task, task.attempt(), now, RetryReason.ACK_TIMEOUT, null,
                "no AcceptedAck within " + options.ackTimeout());
    }

    private void scheduleNextAttempt(RetryTask task, int performedAttempt, Instant now,
                                     RetryReason reason, String errorType, String error) {
        int nextAttempt = performedAttempt + 1;
        if (isBudgetExhausted(nextAttempt)) {
            deadLetter(task, RetryReason.CAP_EXCEEDED, error);
            return;
        }
        retryStore.schedule(task.withNextAttempt(nextAttempt, reason, now, errorType, error));
        log.debug("Scheduled delivery retry for event {}: attempt {}/{}, reason={}",
                task.eventId(), nextAttempt, maxAttempts(), reason);
    }

    private boolean isBudgetExhausted(int attempt) {
        return attempt > maxAttempts();
    }

    private int maxAttempts() {
        return options.maxRetries() + 1;
    }

    private void deadLetter(RetryTask task, RetryReason reason, String error) {
        try {
            lifecycleRepository.onDeadLetter(task.eventId(), task.service(),
                    RetryKind.DELIVERY, reason, null, error);
            log.error("Dead-lettered delivery retry for event {}: {}", task.eventId(), error);
        } catch (RuntimeException e) {
            log.error("Failed to dead-letter delivery retry for event {}", task.eventId(), e);
        }
    }

    private void markDeadWithoutEvent(RetryTask task, String error) {
        log.error("Cannot retry delivery for event {}: {}", task.eventId(), error);
        retryStore.dead(task.eventId(), RetryKind.DELIVERY,
                RetryReason.PUBLISH_FAILED, null, error);
    }

    private Event toRetryEvent(StoredEvent stored, int attempt) {
        Event event = EventUtils.fromJson(stored.payload(), Event.class);
        event = restoreChannels(event, stored.channels());
        return RetryMetadata.stamp(event, stored.eventId(), RetryKind.DELIVERY, attempt);
    }

    /**
     * Re-applies the channels that were routed when the event was first published.
     * The channels are stored separately in the event store (not in the wire
     * format), so a deserialized envelope needs them restored before re-publishing.
     *
     * @param event    the deserialized event
     * @param channels comma-separated channel class names from the store, may be null
     * @return the event with channels restored, or the original if not applicable
     */
    private Event restoreChannels(Event event, String channels) {
        if (channels == null || channels.isBlank() || !(event instanceof Envelope<?> envelope)) {
            return event;
        }
        List<Class<? extends EventChannel>> parsed = parseChannels(channels);
        if (parsed.isEmpty()) {
            return event;
        }
        return new Envelope<>(
                envelope.eventId(), envelope.processId(), envelope.occurredAt(),
                envelope.payload(), envelope.metadata(), parsed);
    }

    private static List<Class<? extends EventChannel>> parseChannels(String names) {
        if (names == null || names.isBlank()) {
            return List.of();
        }
        List<Class<? extends EventChannel>> result = new ArrayList<>();
        for (String name : names.split(",")) {
            String trimmed = name.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                Class<?> type = Class.forName(trimmed, false, DeliveryRetryScheduler.class.getClassLoader());
                if (EventChannel.class.isAssignableFrom(type)) {
                    result.add(type.asSubclass(EventChannel.class));
                }
            } catch (ClassNotFoundException e) {
                log.debug("Unknown channel class '{}' in stored retry metadata", trimmed);
            }
        }
        return List.copyOf(result);
    }

    private static String errorDetails(Throwable error) {
        return error.getMessage() != null ? error.getMessage() : error.getClass().getName();
    }

    @Override
    public void close() {
        stop();
    }
}
