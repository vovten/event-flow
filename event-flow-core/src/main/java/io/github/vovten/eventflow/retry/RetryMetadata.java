package io.github.vovten.eventflow.retry;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;

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
}
