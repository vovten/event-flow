package io.github.vovten.eventflow.publisher.persistence;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.EventPublisherException;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Decorator for {@link EventPublisher} that persists events to database before publishing.
 * <p>
 * This decorator implements the "transactional outbox" pattern:
 * <ol>
 *   <li>Event is serialized and saved to DB with status PENDING</li>
 *   <li>Event is published to the message broker</li>
 *   <li>On success, status is updated to PUBLISHED</li>
 *   <li>On failure, status is updated to FAILED</li>
 * </ol>
 * <p>
 * This ensures no events are lost even if the broker is unavailable.
 * A background job can retry failed events.
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
 * EventPublisher basePublisher = new ChannelEventPublisher(channels);
 * EventRepository repository = new JdbcEventRepository(dataSource);
 * EventPublisher persistentPublisher = new PersistentEventPublisher(
 *     basePublisher, repository, new JsonEventSerializer());
 * 
 * persistentPublisher.publish(event); // Saved to DB, then published
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 * @see EventRepository
 */
public class PersistentEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PersistentEventPublisher.class);

    private final EventPublisher origin;
    private final EventRepository repository;
    private final EventSerializer serializer;

    /**
     * Create persistent publisher with default JSON serializer.
     *
     * @param origin     the delegate publisher
     * @param repository the event repository for persistence
     */
    public PersistentEventPublisher(EventPublisher origin, EventRepository repository) {
        this(origin, repository, new JsonEventSerializer());
    }

    /**
     * Create persistent publisher with custom serializer.
     *
     * @param origin     the delegate publisher
     * @param repository the event repository for persistence
     * @param serializer the event serializer
     */
    public PersistentEventPublisher(EventPublisher origin, EventRepository repository, EventSerializer serializer) {
        this.origin = origin;
        this.repository = repository;
        this.serializer = serializer;
    }

    @Override
    public CompletableFuture<SendResults> publish(Event event) {
        UUID eventId = extractEventId(event);
        String payloadType = event.type().getName();
        UUID processId = extractProcessId(event);
        Instant occurredAt = extractOccurredAt(event);
        byte[] payload = serializer.serialize(event);

        // Create and save record to DB
        EventRecord record = new EventRecord(eventId, payloadType, payload, processId, occurredAt);
        repository.save(record);
        log.debug("Event {} saved to database with status PENDING", eventId);

        // Publish to broker
        return origin.publish(event)
                .thenApply(results -> {
                    // Check if all publishes succeeded
                    if (results.isAllSuccess()) {
                        repository.updateStatus(eventId, EventStatus.PUBLISHED, Instant.now());
                        log.debug("Event {} marked as PUBLISHED", eventId);
                    }
                    return results;
                })
                .exceptionally(ex -> {
                    repository.updateStatus(eventId, EventStatus.FAILED, null);
                    log.error("Event {} marked as FAILED", eventId, ex);
                    throw new EventPublisherException(ex.getMessage(), ex);
                });
    }

    private UUID extractEventId(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.eventId();
        }
        return UUID.randomUUID();
    }

    private UUID extractProcessId(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.processId();
        }
        return null;
    }

    private Instant extractOccurredAt(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.occurredAt();
        }
        return Instant.now();
    }
}
