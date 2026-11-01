package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.retry.RetryKind;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Acknowledgment published after an event handling failure.
 * <p>
 * Published by {@code EventLifecycleDispatcher} back to the source channels
 * of the original event. On the publisher side, {@code AckHandler} processes
 * this event and updates the {@code EventStore} status to {@code FAILED},
 * making it eligible for retry by the {@code EventRetryScheduler}.
 * <p>
 * The retry metadata describes what the consumer-side classifier decided:
 * the retry domain ({@link RetryKind}), the attempt, whether another attempt
 * is allowed, which handlers failed, and the outermost exception type.
 *
 * @param eventId         unique identifier of this ack event
 * @param originalEventId the ID of the original event that failed
 * @param eventType       the type name of the original event
 * @param originalService the name of the service that published the original event
 * @param error           description of the handling error
 * @param sourceChannels  the channels the original event was published to
 * @param processId       the process/correlation ID from the original event
 * @param occurredAt      when this ack was created
 * @param retryKind       the retry context of the dispatch that produced this failure
 *                        ({@code DELIVERY} or {@code PROCESSING})
 * @param attempt         1-based attempt number within that retry context
 * @param retryable       whether another attempt is allowed
 * @param failedHandlers  names of handlers that failed
 * @param errorType       fully qualified class name of the outermost exception, or {@code null}
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public record FailureAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        String error,
        List<Class<? extends EventChannel>> sourceChannels,
        UUID processId,
        Instant occurredAt,
        RetryKind retryKind,
        int attempt,
        boolean retryable,
        List<String> failedHandlers,
        String errorType
) implements LifecycleAckEvent, TraceableEvent {

    public FailureAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(error, "error must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(retryKind, "retryKind must not be null");
        Objects.requireNonNull(failedHandlers, "failedHandlers must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        failedHandlers = List.copyOf(failedHandlers);
    }

    /**
     * Legacy constructor retained for backward compatibility.
     * <p>
     * Defaults: {@code retryKind = PROCESSING}, {@code attempt = 1},
     * {@code retryable = true}, {@code failedHandlers = List.of()},
     * {@code errorType = null}.
     *
     * @param eventId         unique identifier of this ack event
     * @param originalEventId the ID of the original event that failed
     * @param eventType       the type name of the original event
     * @param originalService the name of the service that published the original event
     * @param error           description of the handling error
     * @param sourceChannels  the channels the original event was published to
     * @param processId       the process/correlation ID from the original event
     * @param occurredAt      when this ack was created
     */
    public FailureAck(UUID eventId,
                      UUID originalEventId,
                      String eventType,
                      String originalService,
                      String error,
                      List<Class<? extends EventChannel>> sourceChannels,
                      UUID processId,
                      Instant occurredAt) {
        this(eventId, originalEventId, eventType, originalService, error,
                sourceChannels, processId, occurredAt,
                RetryKind.PROCESSING, 1, true, List.of(), null);
    }

    @Override
    public Class<?> type() {
        return FailureAck.class;
    }

    @Override
    public List<Class<? extends EventChannel>> channels() {
        return sourceChannels;
    }

    @Override
    public UUID eventId() {
        return eventId;
    }
}
