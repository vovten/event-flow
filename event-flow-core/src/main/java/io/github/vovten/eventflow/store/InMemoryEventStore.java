package io.github.vovten.eventflow.store;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
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
            return event.withStatus(status, errorDetails);
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
    public Optional<StoredEvent> findById(UUID eventId) {
        return Optional.ofNullable(store.get(eventId));
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
