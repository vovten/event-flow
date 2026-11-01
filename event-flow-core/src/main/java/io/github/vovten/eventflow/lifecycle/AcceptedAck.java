package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.event.TraceableEvent;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Acknowledgment published when a managed event has been received by the
 * consumer-side dispatcher, before handler execution.
 * <p>
 * This is the delivery/processing boundary: {@code AcceptedAck} confirms
 * <b>delivery only</b>. It does not mean that any handler succeeded. The
 * processing outcome is reported separately by {@link SuccessAck} or
 * {@link FailureAck}.
 * <p>
 * Published by {@code EventLifecycleDispatcher} back to the source channels
 * of the original event. On the publisher side, {@code AckHandler} processes
 * this event and updates the {@code EventStore} status to {@code ACCEPTED}.
 *
 * @param eventId         unique identifier of this ack event
 * @param originalEventId the ID of the original event that was accepted
 * @param eventType       the type name of the original event
 * @param originalService the name of the service that published the original event
 * @param attempt         1-based delivery attempt of the original event
 * @param maxProcessingMs estimated maximum local processing time in milliseconds
 *                        (including local retry backoff), or {@code 0} if unknown
 * @param sourceChannels  the channels the original event was published to
 * @param occurredAt      when this ack was created
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record AcceptedAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        int attempt,
        long maxProcessingMs,
        List<Class<? extends EventChannel>> sourceChannels,
        Instant occurredAt
) implements LifecycleAckEvent, TraceableEvent {

    public AcceptedAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        if (maxProcessingMs < 0) {
            throw new IllegalArgumentException("maxProcessingMs must be >= 0");
        }
    }

    /**
     * This ack intentionally does not carry a process ID.
     * {@link #originalEventId()} is sufficient for lifecycle correlation.
     *
     * @return always {@code null}
     */
    @Override
    public UUID processId() {
        return null;
    }

    @Override
    public Class<?> type() {
        return AcceptedAck.class;
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
