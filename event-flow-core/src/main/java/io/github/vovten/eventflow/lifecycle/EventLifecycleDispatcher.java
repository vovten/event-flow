package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.dispatcher.HandlerResult;
import io.github.vovten.eventflow.dispatcher.HandlerResults;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.retry.RetryKind;
import io.github.vovten.eventflow.retry.RetryMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A decorator for {@link EventDispatcher} that publishes acknowledgment events
 * ({@link AcceptedAck}, {@link SuccessAck}, {@link FailureAck} or
 * {@link NoHandlerAck}) back to the source channels.
 * <p>
 * {@link AcceptedAck} is published before handler execution and confirms
 * delivery only. The final ack reports the processing outcome.
 * <p>
 * The decorator extracts the {@code pubSrv} from the original event's
 * {@link Envelope} metadata and passes it as {@code originalService} in ack
 * events. This enables the publisher-side {@link AckHandler} to filter acks by
 * service identity without querying the database.
 * <p>
 * Lifecycle ack events ({@link LifecycleAckEvent}) are passed through without
 * decoration to prevent infinite loops.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public final class EventLifecycleDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventLifecycleDispatcher.class);

    private static final String PUBLISHER_SERVICE_KEY = "pubSrv";

    private final EventDispatcher origin;
    private final EventPublisher ackPublisher;
    private final LifecycleResolver lifecycleResolver;

    /**
     * Creates a new EventLifecycleDispatcher with the standard lifecycle resolver.
     *
     * @param origin       the dispatcher to delegate to
     * @param ackPublisher the publisher for sending acknowledgment events
     */
    public EventLifecycleDispatcher(EventDispatcher origin, EventPublisher ackPublisher) {
        this(origin, ackPublisher, LifecycleResolver.standard());
    }

    /**
     * Creates a new EventLifecycleDispatcher with a custom lifecycle resolver.
     *
     * @param origin            the dispatcher to delegate to
     * @param ackPublisher      the publisher for sending acknowledgment events
     * @param lifecycleResolver the lifecycle resolver strategy (must not be null)
     */
    public EventLifecycleDispatcher(EventDispatcher origin, EventPublisher ackPublisher,
                                     LifecycleResolver lifecycleResolver) {
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.ackPublisher = Objects.requireNonNull(ackPublisher, "ackPublisher must not be null");
        this.lifecycleResolver = Objects.requireNonNull(lifecycleResolver, "lifecycleResolver must not be null");
    }

    @Override
    public CompletableFuture<HandlerResults> dispatch(Event event) {
        // Prevent infinite loop — don't publish acks for ack events
        if (event instanceof LifecycleAckEvent) {
            log.trace("Passing through lifecycle ack event without wrapping: {}", event);
            return origin.dispatch(event);
        }
        String originalService = extractOriginalService(event);
        RetryContext retryContext = resolveRetryContext(event);
        publishAcceptedAck(event, originalService, retryContext);
        return origin.dispatch(event)
                .whenComplete((results, error) ->
                        publishLifecycleAck(event, originalService, retryContext, results, error));
    }

    private void publishAcceptedAck(Event event, String originalService, RetryContext retryContext) {
        if (shouldSkipLifecycleAck(event)) {
            return;
        }
        UUID eventId = ((TraceableEvent) event).eventId();
        try {
            AcceptedAck accepted = new AcceptedAck(
                    UUID.randomUUID(),
                    eventId,
                    event.type().getName(),
                    originalService,
                    deliveryAttempt(retryContext),
                    0L,
                    event.channels(),
                    Instant.now()
            );
            ackPublisher.publish(accepted);
            log.debug("Published AcceptedAck for event: {} ({})", eventId, event.type().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to publish AcceptedAck for {}", eventId, e);
        }
    }

    private void publishLifecycleAck(Event event, String originalService, RetryContext retryContext,
                                     HandlerResults results, Throwable error) {
        if (shouldSkipLifecycleAck(event)) {
            return;
        }
        if (error != null) {
            publishFailedAck(event, originalService, retryContext, buildErrorChain(error),
                    List.of(), error.getClass().getName());
            return;
        }
        if (results != null && results.isEmpty() && !results.isDuplicate()) {
            publishNoHandlerAck(event, originalService, retryContext);
            return;
        }
        if (hasHandlerFailures(results)) {
            Throwable cause = results.getFirstError().orElse(null);
            List<String> failedHandlers = results.getFailures().stream()
                    .map(HandlerResult::handlerName)
                    .toList();
            publishFailedAck(event, originalService, retryContext, buildErrorChain(cause),
                    failedHandlers, cause != null ? cause.getClass().getName() : null);
            return;
        }
        publishSuccessAck(event, originalService, retryContext);
    }

    private boolean shouldSkipLifecycleAck(Event event) {
        return !(event instanceof TraceableEvent)
                || lifecycleResolver.resolve(event) != EventLifecycle.MANAGED;
    }

    private boolean hasHandlerFailures(HandlerResults results) {
        return results != null && !results.isEmpty() && !results.isAllSuccess();
    }

    /**
     * Builds a condensed exception chain message from outermost to root cause.
     * <p>
     * Format: {@code [{ExceptionClass}] message → [{ExceptionClass}] message → [{RootClass}] root message}
     *
     * @param error the throwable to build chain from, may be null
     * @return formatted chain message, never null
     */
    private String buildErrorChain(Throwable error) {
        if (error == null) {
            return "Handler execution failed";
        }
        StringBuilder sb = new StringBuilder();
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (!sb.isEmpty()) {
                sb.append(" → ");
            }
            String msg = t.getMessage();
            if (msg != null) {
                sb.append("[").append(t.getClass().getSimpleName()).append("] ").append(msg);
            } else {
                sb.append(t.getClass().getSimpleName());
            }
        }
        return sb.toString();
    }

    private void publishSuccessAck(Event event, String originalService, RetryContext retryContext) {
        UUID eventId = ((TraceableEvent) event).eventId();
        try {
            SuccessAck handled = new SuccessAck(
                    UUID.randomUUID(),
                    eventId,
                    event.type().getName(),
                    originalService,
                    event.channels(),
                    null,
                    Instant.now(),
                    retryContext.retryKind(),
                    retryContext.attempt()
            );
            ackPublisher.publish(handled);
            log.debug("Published SuccessAck for event: {} ({})", eventId, event.type().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to publish SuccessAck for {}", eventId, e);
        }
    }

    private void publishFailedAck(Event event, String originalService, RetryContext retryContext,
                                  String errorMessage, List<String> failedHandlers, String errorType) {
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
                    Instant.now(),
                    retryContext.retryKind(),
                    retryContext.attempt(),
                    true,
                    failedHandlers,
                    errorType
            );
            ackPublisher.publish(failed);
            String eventName = event.type().getSimpleName();
            log.warn("Published FailureAck for event: {} ({}) — {}", eventId, eventName, errorMessage);
        } catch (Exception e) {
            log.error("Failed to publish FailureAck for {}", eventId, e);
        }
    }

    private void publishNoHandlerAck(Event event, String originalService, RetryContext retryContext) {
        UUID eventId = ((TraceableEvent) event).eventId();
        try {
            NoHandlerAck noHandler = new NoHandlerAck(
                    UUID.randomUUID(),
                    eventId,
                    event.type().getName(),
                    originalService,
                    deliveryAttempt(retryContext),
                    event.channels(),
                    Instant.now()
            );
            ackPublisher.publish(noHandler);
            log.warn("Published NoHandlerAck for event: {} ({})", eventId, event.type().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to publish NoHandlerAck for {}", eventId, e);
        }
    }

    private RetryContext resolveRetryContext(Event event) {
        if (!(event instanceof Envelope<?> envelope)) {
            return RetryContext.DEFAULT;
        }
        RetryKind retryKind = parseRetryKind(envelope.metadata().get(RetryMetadata.RETRY_KIND));
        int attempt = parseAttempt(envelope.metadata().get(RetryMetadata.ATTEMPT));
        return new RetryContext(retryKind, attempt);
    }

    private RetryKind parseRetryKind(String value) {
        if (StringUtils.isBlank(value)) {
            return RetryKind.PROCESSING;
        }
        try {
            return RetryKind.valueOf(value);
        } catch (IllegalArgumentException e) {
            log.warn("Ignoring unknown retryKind metadata value '{}', defaulting to PROCESSING", value);
            return RetryKind.PROCESSING;
        }
    }

    private int parseAttempt(String value) {
        if (StringUtils.isBlank(value)) {
            return 1;
        }
        try {
            int attempt = Integer.parseInt(value);
            return Math.max(attempt, 1);
        } catch (NumberFormatException e) {
            log.warn("Ignoring invalid attempt metadata value '{}', defaulting to 1", value);
            return 1;
        }
    }

    /**
     * Resolves the delivery attempt from the retry context.
     * <p>
     * Delivery attempts are independent from processing attempts. A
     * {@code PROCESSING} context is a new delivery for an event that was
     * already accepted, so its delivery attempt is {@code 1}.
     *
     * @param retryContext the retry context of the current dispatch
     * @return the delivery attempt number
     */
    private static int deliveryAttempt(RetryContext retryContext) {
        return retryContext.retryKind() == RetryKind.DELIVERY ? retryContext.attempt() : 1;
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

    private record RetryContext(RetryKind retryKind, int attempt) {
        private static final RetryContext DEFAULT = new RetryContext(RetryKind.PROCESSING, 1);
    }
}
