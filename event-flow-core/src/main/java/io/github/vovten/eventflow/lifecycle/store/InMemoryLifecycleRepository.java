package io.github.vovten.eventflow.lifecycle.store;

import io.github.vovten.eventflow.lifecycle.LifecycleRepository;
import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.retry.RetryKind;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * In-memory {@link LifecycleRepository}.
 * <p>
 * The public methods are synchronized, providing atomicity across the two
 * in-memory stores for lifecycle transitions performed through this repository.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class InMemoryLifecycleRepository implements LifecycleRepository {

    private static final Set<EventStatus> PUBLISH_FAILURE_SOURCES =
            Set.of(EventStatus.NEW, EventStatus.FAILED);

    private static final Set<EventStatus> ACCEPTANCE_SOURCES =
            Set.of(EventStatus.NEW, EventStatus.PUBLISHED);

    private static final Set<EventStatus> PROCESSING_SOURCES =
            Set.of(EventStatus.NEW, EventStatus.PUBLISHED, EventStatus.ACCEPTED, EventStatus.FAILED);

    private static final Set<EventStatus> MANUAL_RETRY_SOURCES =
            Set.of(EventStatus.values());

    private final EventStore eventStore;
    private final EventRetryStore retryStore;

    /**
     * Creates a repository over the given stores.
     *
     * @param eventStore the event store
     * @param retryStore the durable retry store
     */
    public InMemoryLifecycleRepository(EventStore eventStore, EventRetryStore retryStore) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.retryStore = Objects.requireNonNull(retryStore, "retryStore must not be null");
    }

    @Override
    public synchronized void onPublishFailed(UUID eventId, String service, RetryReason reason,
                                             Instant nextAttemptAt, String errorType, String error) {
        boolean applied = eventStore.transitionStatus(
                eventId, PUBLISH_FAILURE_SOURCES, EventStatus.FAILED, error);
        if (applied) {
            schedule(eventId, service, RetryKind.DELIVERY, 1, reason, nextAttemptAt, null);
        }
    }

    @Override
    public synchronized void onAccepted(UUID eventId, int deliveryAttempt) {
        eventStore.transitionStatus(eventId, ACCEPTANCE_SOURCES, EventStatus.ACCEPTED, null);
        retryStore.complete(eventId, RetryKind.DELIVERY);
    }

    @Override
    public synchronized void onProcessingFailed(UUID eventId, String service, int attempt,
                                                boolean retryable, Instant nextAttemptAt,
                                                String policyName, String errorType, String error) {
        EventStatus target = retryable ? EventStatus.FAILED : EventStatus.DEAD;
        boolean applied = eventStore.transitionStatus(
                eventId, PROCESSING_SOURCES, target, error);
        if (!applied) {
            return;
        }
        retryStore.complete(eventId, RetryKind.DELIVERY);
        if (retryable) {
            schedule(eventId, service, RetryKind.PROCESSING, attempt,
                    RetryReason.HANDLER_FAILED, nextAttemptAt, policyName);
        } else {
            deadOrSchedule(eventId, service, RetryKind.PROCESSING,
                    RetryReason.HANDLER_FAILED, policyName, errorType, error);
        }
    }

    @Override
    public synchronized void onProcessingDisabled(UUID eventId, String errorType, String error) {
        eventStore.transitionStatus(eventId, PROCESSING_SOURCES, EventStatus.FAILED, error);
        retryStore.complete(eventId, RetryKind.DELIVERY);
    }

    @Override
    public synchronized void onHandled(UUID eventId, int attempt) {
        boolean applied = eventStore.transitionStatus(
                eventId, PROCESSING_SOURCES, EventStatus.HANDLED, null);
        if (applied) {
            retryStore.complete(eventId, RetryKind.DELIVERY);
            retryStore.complete(eventId, RetryKind.PROCESSING);
        }
    }

    @Override
    public synchronized void onNoHandler(UUID eventId, String service, int attempt,
                                         String errorType, String error) {
        boolean applied = eventStore.transitionStatus(
                eventId, PROCESSING_SOURCES, EventStatus.DEAD, error);
        if (applied) {
            retryStore.complete(eventId, RetryKind.DELIVERY);
            deadOrSchedule(eventId, service, RetryKind.PROCESSING,
                    RetryReason.NO_HANDLER, null, errorType, error);
        }
    }

    @Override
    public synchronized void onDeadLetter(UUID eventId, String service, RetryKind kind,
                                          RetryReason reason, String errorType, String error) {
        boolean applied = eventStore.transitionStatus(
                eventId, PROCESSING_SOURCES, EventStatus.DEAD, error);
        if (applied) {
            deadOrSchedule(eventId, service, kind, reason, null, errorType, error);
        }
    }

    @Override
    public synchronized void markForRetry(UUID eventId, String service) {
        boolean applied = eventStore.transitionStatus(
                eventId, MANUAL_RETRY_SOURCES, EventStatus.NEW, null);
        if (!applied) {
            return;
        }
        RetryKind kind = resolveKind(eventId);
        Optional<RetryTask> existing = retryStore.find(eventId, kind);
        String policyName = existing.map(RetryTask::policyName).orElse(null);
        schedule(eventId, service, kind, 1, RetryReason.HANDLER_FAILED,
                Instant.now(), policyName);
    }

    private void schedule(UUID eventId, String service, RetryKind kind, int attempt,
                          RetryReason reason, Instant nextAttemptAt, String policyName) {
        retryStore.schedule(RetryTask.pending(
                eventId, service, kind, attempt, reason, nextAttemptAt, policyName));
    }

    private void deadOrSchedule(UUID eventId, String service, RetryKind kind,
                                RetryReason reason, String policyName,
                                String errorType, String error) {
        if (retryStore.find(eventId, kind).isPresent()) {
            retryStore.dead(eventId, kind, reason, errorType, error);
            return;
        }
        RetryTask dead = RetryTask.pending(
                        eventId, service, kind, 1, reason, Instant.now(), policyName)
                .withDead(reason, errorType, error);
        retryStore.schedule(dead);
    }

    private RetryKind resolveKind(UUID eventId) {
        if (retryStore.find(eventId, RetryKind.PROCESSING).isPresent()) {
            return RetryKind.PROCESSING;
        }
        if (retryStore.find(eventId, RetryKind.DELIVERY).isPresent()) {
            return RetryKind.DELIVERY;
        }
        return RetryKind.PROCESSING;
    }
}
