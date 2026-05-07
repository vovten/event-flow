package io.github.vovten.eventflow.publisher.persistence;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Event publisher decorator that persists events to database before publishing.
 * <p>
 * Implements the transactional outbox pattern:
 * <ol>
 *   <li>Serialize full event (Envelope or raw Event) to JSON and save with PENDING status</li>
 *   <li>Publish event to destination</li>
 *   <li>Update status to PUBLISHED or FAILED</li>
 * </ol>
 * <p>
 * Events can be retried later by setting {@code retry = true} in the database.
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public class PersistentEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PersistentEventPublisher.class);

    private final EventPublisher delegate;
    private final EventRepository repository;
    private final EventSerializer serializer;

    public PersistentEventPublisher(EventPublisher delegate, EventRepository repository, EventSerializer serializer) {
        this.delegate = delegate;
        this.repository = repository;
        this.serializer = serializer;
    }

    @Override
    public CompletableFuture<SendResults> publish(Event event) {
        return doPublish(event, null);
    }

    @Override
    public <T> CompletableFuture<SendResults> publish(T payload) {
        if (payload instanceof Envelope) {
            @SuppressWarnings("unchecked")
            Envelope<T> envelope = (Envelope<T>) payload;
            return doPublish(envelope, envelope);
        }
        return doPublish(payload, null);
    }

    private <T> CompletableFuture<SendResults> doPublish(T event, Envelope<?> envelope) {
        // Extract event ID, processId, and serialize the full event (Envelope or raw Event)
        UUID eventId;
        UUID processId;
        Event eventToSerialize;
        
        if (envelope != null) {
            // If we have an envelope, serialize the entire Envelope
            eventId = envelope.eventId();
            processId = envelope.processId();
            eventToSerialize = (Event) envelope;
        } else if (event instanceof Event) {
            eventToSerialize = (Event) event;
            if (eventToSerialize instanceof TraceableEvent) {
                TraceableEvent te = (TraceableEvent) eventToSerialize;
                eventId = te.eventId();
                processId = te.processId();
            } else {
                eventId = UUID.randomUUID();
                processId = null;
            }
        } else {
            eventId = UUID.randomUUID();
            processId = null;
            eventToSerialize = null;
        }

        // Serialize to JSON - serialize the full event (Envelope or Event)
        String payloadJson;
        if (eventToSerialize != null) {
            byte[] serialized = serializer.serialize(eventToSerialize);
            payloadJson = new String(serialized, StandardCharsets.UTF_8);
        } else {
            payloadJson = "{}";
        }

        // Save to outbox with PENDING status
        EventRecord record = EventRecord.create(eventId, processId, payloadJson);
        repository.save(record);
        log.debug("Event {} saved to outbox with PENDING status (processId={})", eventId, processId);

        // Publish to destination
        return delegate.publish(event)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed to publish event {}, marking as FAILED", eventId, error);
                        repository.updateStatus(eventId, EventStatus.FAILED, error.getMessage());
                    } else {
                        log.debug("Event {} published successfully", eventId);
                        repository.updateStatus(eventId, EventStatus.PUBLISHED);
                    }
                });
    }
}