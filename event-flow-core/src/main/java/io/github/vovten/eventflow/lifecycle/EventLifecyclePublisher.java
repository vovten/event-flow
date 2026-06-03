package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.util.EventUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A decorator for {@link EventPublisher} that persists events to an {@link EventStore}
 * and manages their lifecycle status according to the event's {@link EventLifecycle} level.
 * <p>
 * Behaviour by lifecycle level:
 * <ul>
 *   <li>{@link EventLifecycle#NONE} — passes through without persistence</li>
 *   <li>{@link EventLifecycle#PERSISTED} — persists the event with
 *       {@link EventStatus#UNDEFINED} status, no further tracking</li>
 *   <li>{@link EventLifecycle#MANAGED} — persists with {@link EventStatus#NEW},
 *       tracks publication outcome ({@link EventStatus#PUBLISHED} / {@link EventStatus#FAILED})
 *       and publishes lifecycle ack events for end-to-end tracking</li>
 * </ul>
 * <p>
 * If configured with a {@code service} name, it enriches the event's
 * {@link Envelope} metadata with the service identity ({@code publisherService})
 * so that acknowledgment events can be filtered by the originating service.
 * <p>
 * {@link LifecycleAckEvent} instances are passed through without persistence
 * (they are technical events used for lifecycle tracking, not business events).
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public final class EventLifecyclePublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventLifecyclePublisher.class);

    private static final String PUBLISHER_SERVICE_KEY = "publisherService";

    private final String service;
    private final EventPublisher origin;
    private final EventStore eventStore;

    /**
     * Creates a new EventLifecyclePublisher.
     *
     * @param origin     the underlying publisher to delegate to
     * @param eventStore the event store for persistence
     * @param service    the service name for ack filtering, or null/empty to disable
     */
    public EventLifecyclePublisher(EventPublisher origin, EventStore eventStore, String service) {
        this.service = service;
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
    }

    @Override
    public CompletableFuture<SendResults> publish(Event event) {
        Objects.requireNonNull(event, "event must not be null");

        if (shouldSkipPersistence(event)) {
            return origin.publish(event);
        }
        Event enriched = enrichWithService(event);
        UUID eventId = resolveEventId(enriched);
        EventLifecycle lifecycle = EventUtils.lifecycle(event);

        if (lifecycle == EventLifecycle.PERSISTED) {
            // Persist with UNDEFINED status, no further tracking
            persistNewOnly(eventId, enriched);
            return origin.publish(enriched);
        }
        // MANAGED — persist with NEW status, full lifecycle tracking
        persistOrReset(eventId, enriched);
        return origin.publish(enriched)
                .whenComplete((result, error) -> updatePublishResult(eventId, result, error));
    }

    private boolean shouldSkipPersistence(Event event) {
        if (event instanceof LifecycleAckEvent) {
            log.trace("Skipping persistence for lifecycle ack event: {}", event);
            return true;
        }
        EventLifecycle lifecycle = EventUtils.lifecycle(event);
        if (lifecycle == EventLifecycle.NONE) {
            log.trace("Skipping persistence for NONE lifecycle event: {}", event);
            return true;
        }
        return false;
    }

    private void persistNewOnly(UUID eventId, Event event) {
        Optional<StoredEvent> existing = eventStore.findById(eventId);
        if (existing.isPresent()) {
            log.trace("Event already persisted, skipping: {} ({})", eventId, resolveEventType(event));
            return;
        }
        String eventType = resolveEventType(event);
        UUID processId = resolveProcessId(event);
        String payload = EventUtils.toJson(event);
        StoredEvent stored = StoredEvent.newEvent(eventId, eventType, payload, processId, EventStatus.UNDEFINED);
        eventStore.save(stored);
        log.debug("Saved new event with UNDEFINED status: {} ({})", eventId, eventType);
    }

    private void persistOrReset(UUID eventId, Event event) {
        String eventType = resolveEventType(event);
        Optional<StoredEvent> existing = eventStore.findById(eventId);
        if (existing.isPresent()) {
            eventStore.updateStatus(eventId, EventStatus.NEW, null);
            log.debug("Reset event status for retry: {} ({})", eventId, eventType);
            return;
        }
        UUID processId = resolveProcessId(event);
        String payload = EventUtils.toJson(event);
        StoredEvent stored = StoredEvent.newEvent(eventId, eventType, payload, processId, EventStatus.NEW);
        eventStore.save(stored);
        log.debug("Saved new event to store: {} ({})", eventId, eventType);
    }

    private void updatePublishResult(UUID eventId, SendResults result, Throwable error) {
        if (error != null) {
            eventStore.updateStatus(eventId, EventStatus.FAILED, error.getMessage());
            log.warn("Event publication failed: {} — {}", eventId, error.getMessage());
            return;
        }
        if (result == null || result.isAllFailure()) {
            String errorMsg = result != null ? result.getSummary() : "null result";
            eventStore.updateStatus(eventId, EventStatus.FAILED, errorMsg);
            log.warn("Event publication failed: {} — {}", eventId, errorMsg);
            return;
        }
        eventStore.updateStatus(eventId, EventStatus.PUBLISHED, null);
        log.debug("Event published successfully: {}", eventId);
    }

    private Event enrichWithService(Event event) {
        if (StringUtils.isEmpty(service) || !(event instanceof Envelope<?> env)) {
            return event;
        }
        Map<String, String> newMetadata = new HashMap<>(env.metadata());
        newMetadata.put(PUBLISHER_SERVICE_KEY, service);
        return new Envelope<>(
                env.eventId(),
                env.processId(),
                env.occurredAt(),
                env.payload(),
                newMetadata
        );
    }

    private UUID resolveEventId(Event event) {
        if (event instanceof TraceableEvent traceable) {
            return traceable.eventId();
        }
        return UUID.randomUUID();
    }

    private UUID resolveProcessId(Event event) {
        if (event instanceof TraceableEvent traceable) {
            return traceable.processId();
        }
        return null;
    }

    private String resolveEventType(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload().getClass().getSimpleName();
        }
        return event.getClass().getSimpleName();
    }
}
