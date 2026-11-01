package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.event.TraceableEvent;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Acknowledgment published when a managed event has been received by the
 * consumer-side dispatcher, but no handler was resolved for it.
 * <p>
 * This is a permanent configuration failure, not a transient delivery or
 * processing failure. Retrying the event produces the same result. On the
 * publisher side, {@code AckHandler} processes this event and moves the
 * {@code EventStore} status to the terminal {@code DEAD} state.
 * <p>
 * Published by {@code EventLifecycleDispatcher} back to the source channels
 * of the original event.
 *
 * @param eventId         unique identifier of this ack event
 * @param originalEventId the ID of the original event that had no handler
 * @param eventType       the type name of the original event
 * @param originalService the name of the service that published the original event
 * @param attempt         1-based delivery attempt of the original event
 * @param sourceChannels  the channels the original event was published to
 * @param occurredAt      when this ack was created
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record NoHandlerAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        int attempt,
        List<Class<? extends EventChannel>> sourceChannels,
        Instant occurredAt
) implements LifecycleAckEvent, TraceableEvent {

    public NoHandlerAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
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
        return NoHandlerAck.class;
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
