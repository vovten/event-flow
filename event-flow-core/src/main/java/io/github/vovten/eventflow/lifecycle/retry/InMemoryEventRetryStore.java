package io.github.vovten.eventflow.lifecycle.retry;

import io.github.vovten.eventflow.retry.RetryKind;

import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link EventRetryStore}.
 * <p>
 * Intended for tests and single-JVM scenarios. All mutating operations are
 * synchronized so claim/reschedule transitions are atomic.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class InMemoryEventRetryStore implements EventRetryStore {

    private final Map<RetryKey, RetryTask> tasks = new ConcurrentHashMap<>();

    @Override
    public synchronized void schedule(RetryTask task) {
        Objects.requireNonNull(task, "task must not be null");
        tasks.put(new RetryKey(task.eventId(), task.retryKind()), task);
    }

    @Override
    public synchronized List<RetryTask> findDue(RetryKind retryKind, String service,
                                                Instant now, int batchSize) {
        Objects.requireNonNull(retryKind, "retryKind must not be null");
        Objects.requireNonNull(service, "service must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (batchSize <= 0) {
            return List.of();
        }
        return tasks.values().stream()
                .filter(task -> task.retryKind() == retryKind)
                .filter(task -> service.equals(task.service()))
                .filter(task -> task.isDue(now) || task.isClaimExpired(now))
                .sorted(Comparator.comparing(RetryTask::nextAttemptAt))
                .limit(batchSize)
                .toList();
    }

    @Override
    public synchronized boolean claim(UUID eventId, RetryKind retryKind,
                                      String owner, Instant claimedUntil) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(retryKind, "retryKind must not be null");
        Objects.requireNonNull(owner, "owner must not be null");
        Objects.requireNonNull(claimedUntil, "claimedUntil must not be null");
        Instant now = Instant.now();
        RetryKey key = new RetryKey(eventId, retryKind);
        RetryTask task = tasks.get(key);
        if (task == null || !(task.isDue(now) || task.isClaimExpired(now))) {
            return false;
        }
        tasks.put(key, task.withClaim(owner, claimedUntil));
        return true;
    }

    @Override
    public synchronized void reschedule(UUID eventId, RetryKind retryKind, int attempt,
                                        Instant nextAttemptAt, String errorType, String error) {
        RetryKey key = new RetryKey(eventId, retryKind);
        RetryTask task = tasks.get(key);
        if (task == null) {
            throw new NoSuchElementException("Retry task not found: " + eventId + "/" + retryKind);
        }
        tasks.put(key, task.withReschedule(attempt, nextAttemptAt, errorType, error));
    }

    @Override
    public synchronized void complete(UUID eventId, RetryKind retryKind) {
        tasks.remove(new RetryKey(eventId, retryKind));
    }

    @Override
    public synchronized void dead(UUID eventId, RetryKind retryKind, RetryReason reason,
                                  String errorType, String error) {
        RetryKey key = new RetryKey(eventId, retryKind);
        RetryTask task = tasks.get(key);
        if (task == null) {
            throw new NoSuchElementException("Retry task not found: " + eventId + "/" + retryKind);
        }
        tasks.put(key, task.withDead(reason, errorType, error));
    }

    @Override
    public synchronized Optional<RetryTask> find(UUID eventId, RetryKind retryKind) {
        return Optional.ofNullable(tasks.get(new RetryKey(eventId, retryKind)));
    }

    @Override
    public synchronized int deleteByEventIds(Collection<UUID> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return 0;
        }
        Set<UUID> ids = Set.copyOf(eventIds);
        int before = tasks.size();
        tasks.keySet().removeIf(key -> ids.contains(key.eventId()));
        return before - tasks.size();
    }

    /**
     * Returns the number of stored rows (for tests).
     *
     * @return the row count
     */
    synchronized int size() {
        return tasks.size();
    }

    private record RetryKey(UUID eventId, RetryKind retryKind) {
    }
}
