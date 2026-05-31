package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.event.TraceableEvent;

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
 *
 * @param eventId         unique identifier of this ack event
 * @param originalEventId the ID of the original event that failed
 * @param eventType       the type name of the original event
 * @param originalService the name of the service that published the original event
 * @param error           description of the handling error
 * @param sourceChannels  the channels the original event was published to
 * @param processId       the process/correlation ID from the original event
 * @param occurredAt      when this ack was created
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public record FailureAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        String error,
        List<Class<? extends EventChannel>> sourceChannels,
        UUID processId,
        Instant occurredAt
) implements LifecycleAckEvent, TraceableEvent {

    public FailureAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(error, "error must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
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
