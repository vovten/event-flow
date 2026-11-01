package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.retry.RetryKind;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Acknowledgment published after an event has been successfully handled
 * by all registered handlers.
 * <p>
 * Published by {@code EventLifecycleDispatcher} back to the source channels
 * of the original event. On the publisher side, {@code AckHandler} processes
 * this event and updates the {@code EventStore} status to {@code HANDLED}.
 *
 * @param eventId         unique identifier of this ack event
 * @param originalEventId the ID of the original event that was handled
 * @param eventType       the type name of the original event
 * @param originalService the name of the service that published the original event
 * @param sourceChannels  the channels the original event was published to
 * @param processId       the process/correlation ID from the original event
 * @param occurredAt      when this ack was created
 * @param retryKind       the retry context of the dispatch that produced this success
 *                        ({@code DELIVERY} or {@code PROCESSING})
 * @param attempt         1-based attempt number within that retry context
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public record SuccessAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        List<Class<? extends EventChannel>> sourceChannels,
        UUID processId,
        Instant occurredAt,
        RetryKind retryKind,
        int attempt
) implements LifecycleAckEvent, TraceableEvent {

    public SuccessAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(retryKind, "retryKind must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
    }

    /**
     * Legacy constructor retained for backward compatibility.
     * <p>
     * Defaults: {@code retryKind = PROCESSING}, {@code attempt = 1}.
     *
     * @param eventId         unique identifier of this ack event
     * @param originalEventId the ID of the original event that was handled
     * @param eventType       the type name of the original event
     * @param originalService the name of the service that published the original event
     * @param sourceChannels  the channels the original event was published to
     * @param processId       the process/correlation ID from the original event
     * @param occurredAt      when this ack was created
     */
    public SuccessAck(UUID eventId,
                      UUID originalEventId,
                      String eventType,
                      String originalService,
                      List<Class<? extends EventChannel>> sourceChannels,
                      UUID processId,
                      Instant occurredAt) {
        this(eventId, originalEventId, eventType, originalService,
                sourceChannels, processId, occurredAt,
                RetryKind.PROCESSING, 1);
    }

    @Override
    public Class<?> type() {
        return SuccessAck.class;
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
