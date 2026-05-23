package io.github.vovten.eventflow.event.lifecycle;

import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.dispatcher.HandlerResults;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.util.EventUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A decorator for {@link EventDispatcher} that publishes acknowledgment events
 * ({@link SuccessAck} or {@link FailureAck}) back to the source channels
 * after an event has been processed by the dispatcher.
 * <p>
 * The decorator extracts the {@code publisherService} from the original event's
 * {@link Envelope} metadata and passes it as {@code originalService} in ack events.
 * This enables the publisher-side {@link AckHandler} to filter acks by service
 * identity without querying the database.
 * <p>
 * Lifecycle ack events ({@link LifecycleAckEvent}) are passed through without
 * decoration to prevent infinite loops.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public final class EventLifecycleDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventLifecycleDispatcher.class);

    private static final String PUBLISHER_SERVICE_KEY = "publisherService";

    private final EventDispatcher origin;
    private final EventPublisher ackPublisher;

    /**
     * Creates a new EventLifecycleDispatcher.
     *
     * @param origin       the dispatcher to delegate to
     * @param ackPublisher the publisher for sending acknowledgment events
     */
    public EventLifecycleDispatcher(EventDispatcher origin, EventPublisher ackPublisher) {
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.ackPublisher = Objects.requireNonNull(ackPublisher, "ackPublisher must not be null");
    }

    @Override
    public CompletableFuture<HandlerResults> dispatch(Event event) {
        // Prevent infinite loop — don't publish acks for ack events
        if (event instanceof LifecycleAckEvent) {
            log.trace("Passing through lifecycle ack event without wrapping: {}", event);
            return origin.dispatch(event);
        }
        String originalService = extractOriginalService(event);
        return origin.dispatch(event)
                .whenComplete((results, error) ->
                        publishLifecycleAck(event, originalService, results, error));
    }

    private void publishLifecycleAck(Event event, String originalService,
                                     HandlerResults results, Throwable error) {
        if (shouldSkipLifecycleAck(event)) {
            return;
        }
        if (error != null) {
            publishFailedAck(event, originalService, error.getMessage());
            return;
        }
        if (hasHandlerFailures(results)) {
            publishFailedAck(event, originalService, extractErrorSummary(results));
            return;
        }
        publishSuccessAck(event, originalService);
    }

    private boolean shouldSkipLifecycleAck(Event event) {
        return !(event instanceof TraceableEvent)
                || EventUtils.lifecycle(event) != EventLifecycle.FULL;
    }

    private boolean hasHandlerFailures(HandlerResults results) {
        return results != null && !results.isEmpty() && !results.isAllSuccess();
    }

    private String extractErrorSummary(HandlerResults results) {
        return results.getFirstError()
                .map(Throwable::getMessage)
                .filter(Objects::nonNull)
                .orElse("Handler execution failed");
    }

    private void publishSuccessAck(Event event, String originalService) {
        UUID eventId = ((TraceableEvent) event).eventId();
        try {
            SuccessAck handled = new SuccessAck(
                    UUID.randomUUID(),
                    eventId,
                    event.type().getName(),
                    originalService,
                    event.channels(),
                    null,
                    Instant.now()
            );
            ackPublisher.publish(handled);
            log.debug("Published SuccessAck for event: {} ({})", eventId, event.type().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to publish SuccessAck for {}", eventId, e);
        }
    }

    private void publishFailedAck(Event event, String originalService, String errorMessage) {
        UUID eventId = ((TraceableEvent) event).eventId();
        try {
            FailureAck failed = new FailureAck(
                    UUID.randomUUID(),
                    eventId,
                    event.type().getName(),
                    originalService,
                    errorMessage,
                    event.channels(),
                    null,
                    Instant.now()
            );
            ackPublisher.publish(failed);
            String eventName = event.type().getSimpleName();
            log.warn("Published FailureAck for event: {} ({}) — {}", eventId, eventName, errorMessage);
        } catch (Exception e) {
            log.error("Failed to publish FailureAck for {}", eventId, e);
        }
    }

    /**
     * Extracts the publisher service name from an event's Envelope metadata.
     *
     * @param event the event
     * @return the service name, or null if not present
     */
    private String extractOriginalService(Event event) {
        if (event instanceof Envelope<?> env) {
            Object value = env.metadata().get(PUBLISHER_SERVICE_KEY);
            if (value instanceof String s && StringUtils.isNotBlank(s)) {
                return s;
            }
        }
        return null;
    }

    @Override
    public void register(Object listener) {
        origin.register(listener);
    }

    @Override
    public boolean isRegistered(Object listener) {
        return origin.isRegistered(listener);
    }

    @Override
    public void start(Consumer<Event> dispatchConsumer) {
        origin.start(dispatchConsumer);
    }

    @Override
    public void stop() {
        origin.stop();
    }
}
