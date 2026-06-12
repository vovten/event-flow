package io.github.vovten.eventflow.lifecycle.store;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toList;

/**
 * In-memory implementation of {@link EventStore} backed by a {@link ConcurrentHashMap}.
 * <p>
 * Suitable for testing and single-JVM scenarios where persistence across restarts
 * is not required.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public class InMemoryEventStore implements EventStore {

    private final Map<UUID, StoredEvent> store = new ConcurrentHashMap<>();

    @Override
    public String getType() {
        return "in-memory";
    }

    @Override
    public void save(StoredEvent event) {
        StoredEvent previous = store.putIfAbsent(event.eventId(), event);
        if (previous != null) {
            throw new IllegalArgumentException(
                    "Event with ID " + event.eventId() + " already exists");
        }
    }

    @Override
    public void updateStatus(UUID eventId, EventStatus status, String errorDetails) {
        StoredEvent existing = store.computeIfPresent(eventId, (id, event) -> {
            if (status == EventStatus.NEW) {
                return event.withRetry().withStatus(EventStatus.NEW, errorDetails);
            }
            return event.withStatus(status, errorDetails).withRetryFlag(false);
        });
        if (existing == null) {
            throw new NoSuchElementException("Event not found: " + eventId);
        }
    }

    @Override
    public List<StoredEvent> findByStatus(EventStatus status, Instant before) {
        return store.values().stream()
                .filter(e -> e.status() == status)
                .filter(e -> e.updatedAt().isBefore(before))
                .collect(toList());
    }

    @Override
    public List<StoredEvent> findByStatuses(List<EventStatus> statuses, Instant before, int batchSize) {
        if (statuses.isEmpty() || batchSize <= 0) {
            return List.of();
        }
        Set<EventStatus> statusSet = EnumSet.copyOf(statuses);
        return store.values().stream()
                .filter(e -> statusSet.contains(e.status()))
                .filter(e -> e.updatedAt().isBefore(before))
                .limit(batchSize)
                .collect(toList());
    }

    @Override
    public void markForRetry(UUID eventId) {
        StoredEvent existing = store.computeIfPresent(eventId, (id, event) ->
                event.withRetryFlag(true).withStatus(event.status(), null)
        );
        if (existing == null) {
            throw new NoSuchElementException("Event not found: " + eventId);
        }
    }

    @Override
    public List<StoredEvent> findRetryableEvents(List<EventStatus> statuses, Instant before, int batchSize) {
        if (statuses.isEmpty() || batchSize <= 0) {
            return List.of();
        }
        Set<EventStatus> statusSet = EnumSet.copyOf(statuses);
        return store.values().stream()
                .filter(e -> statusSet.contains(e.status()) || e.retry())
                .filter(e -> e.updatedAt().isBefore(before))
                .limit(batchSize)
                .collect(toList());
    }

    @Override
    public Optional<StoredEvent> findById(UUID eventId) {
        return Optional.ofNullable(store.get(eventId));
    }

    @Override
    public int deleteByStatuses(List<EventStatus> statuses, Instant before, int batchSize) {
        Set<EventStatus> statusSet = EnumSet.copyOf(statuses);
        int[] deletedCount = {0};
        store.values().removeIf(event -> {
            if (statusSet.contains(event.status()) && event.updatedAt().isBefore(before)) {
                deletedCount[0]++;
                return true;
            }
            return false;
        });
        return deletedCount[0];
    }

    /**
     * Returns the total number of stored events (for testing).
     *
     * @return number of stored events
     */
    int size() {
        return store.size();
    }

    /**
     * Removes all events from the store (for testing).
     */
    void clear() {
        store.clear();
    }
}
