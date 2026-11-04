package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.retry.RetryKind;
import io.github.vovten.eventflow.retry.RetryMetadata;
import io.github.vovten.eventflow.util.EventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Reconstructs a stored event for a scheduler-initiated re-publish.
 * <p>
 * Deserializes the payload, restores the channel routing stored in the event
 * store, and stamps the retry context into the envelope metadata.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
final class RetryEventFactory {

    private static final Logger log = LoggerFactory.getLogger(RetryEventFactory.class);

    private RetryEventFactory() {
    }

    /**
     * Builds a re-publishable event with the given retry context.
     *
     * @param stored    the stored event
     * @param retryKind retry domain of the dispatch
     * @param attempt   1-based attempt number
     * @return the event ready for re-publishing
     */
    static Event forRetry(StoredEvent stored, RetryKind retryKind, int attempt) {
        Event event = EventUtils.fromJson(stored.payload(), Event.class);
        event = restoreChannels(event, stored.channels());
        return RetryMetadata.stamp(event, stored.eventId(), retryKind, attempt);
    }

    /**
     * Re-applies the channels that were routed when the event was first published.
     * The channels are stored separately in the event store (not in the wire
     * format), so a deserialized envelope needs them restored before re-publishing.
     *
     * @param event    the deserialized event
     * @param channels comma-separated channel class names from the store, may be null
     * @return the event with channels restored, or the original if not applicable
     */
    private static Event restoreChannels(Event event, String channels) {
        if (channels == null || channels.isBlank() || !(event instanceof Envelope<?> envelope)) {
            return event;
        }
        List<Class<? extends EventChannel>> parsed = parseChannels(channels);
        if (parsed.isEmpty()) {
            return event;
        }
        return new Envelope<>(
                envelope.eventId(), envelope.processId(), envelope.occurredAt(),
                envelope.payload(), envelope.metadata(), parsed);
    }

    private static List<Class<? extends EventChannel>> parseChannels(String names) {
        if (names == null || names.isBlank()) {
            return List.of();
        }
        List<Class<? extends EventChannel>> result = new ArrayList<>();
        for (String name : names.split(",")) {
            String trimmed = name.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                Class<?> type = Class.forName(trimmed, false, RetryEventFactory.class.getClassLoader());
                if (EventChannel.class.isAssignableFrom(type)) {
                    result.add(type.asSubclass(EventChannel.class));
                }
            } catch (ClassNotFoundException e) {
                log.debug("Unknown channel class '{}' in stored retry metadata", trimmed);
            }
        }
        return List.copyOf(result);
    }
}
