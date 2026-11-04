package io.github.vovten.eventflow.retry;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Envelope metadata keys used to correlate acknowledgments with the retry
 * context of the dispatch that produced them.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetryMetadata {

    /**
     * Retry context of the dispatch: {@link RetryKind#DELIVERY} or
     * {@link RetryKind#PROCESSING}.
     */
    public static final String RETRY_KIND = "retryKind";

    /**
     * 1-based attempt number within the retry context.
     */
    public static final String ATTEMPT = "attempt";

    private RetryMetadata() {
    }

    /**
     * Reads the current attempt number from the event's Envelope metadata.
     * Absent or invalid values default to {@code 1}.
     *
     * @param event the event to inspect
     * @return the 1-based attempt number
     */
    public static int attempt(Event event) {
        if (event instanceof Envelope<?> envelope) {
            String value = envelope.metadata().get(ATTEMPT);
            if (value != null && !value.isBlank()) {
                try {
                    int attempt = Integer.parseInt(value);
                    return attempt >= 1 ? attempt : 1;
                } catch (NumberFormatException ignored) {
                    return 1;
                }
            }
        }
        return 1;
    }

    /**
     * Stamps the retry context into an event.
     * <p>
     * Envelopes receive two extra metadata entries. A raw event is wrapped in
     * a new {@link Envelope} so the consumer can echo the retry context in its
     * acknowledgment. Raw events have no envelope metadata of their own, so the
     * stored event identifier is used for the wrapper.
     *
     * @param event     the event to stamp
     * @param eventId   identifier of the stored event
     * @param retryKind retry context of the dispatch
     * @param attempt   1-based attempt number
     * @return the stamped event
     * @throws IllegalArgumentException if {@code attempt} is less than 1
     */
    public static Event stamp(Event event, UUID eventId, RetryKind retryKind, int attempt) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(retryKind, "retryKind must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        if (event instanceof Envelope<?> envelope) {
            return envelope
                    .withAdditionalMetadata(RETRY_KIND, retryKind.name())
                    .withAdditionalMetadata(ATTEMPT, Integer.toString(attempt));
        }
        UUID processId = event instanceof TraceableEvent traceable ? traceable.processId() : null;
        Instant occurredAt = event instanceof TraceableEvent traceable ? traceable.occurredAt() : Instant.now();
        Map<String, String> metadata = Map.of(
                RETRY_KIND, retryKind.name(),
                ATTEMPT, Integer.toString(attempt));
        return new Envelope<>(eventId, processId, occurredAt, event, metadata, event.channels());
    }
}
