package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.serialization.EventSerializationException;
import org.apache.kafka.common.errors.RecordTooLargeException;

/**
 * Default {@link RetryClassifier}.
 * <p>
 * Retries transient failures (network errors, timeouts, broker unavailability,
 * a full local queue) and refuses deterministic ones, because a retry would
 * produce the same result and only spend the retry budget:
 * <ul>
 *   <li>{@link EventPublisherConfigException} — configuration errors;</li>
 *   <li>{@link IllegalArgumentException} and {@link NullPointerException} —
 *       programmer errors;</li>
 *   <li>{@link EventSerializationException} and Kafka
 *       {@code SerializationException} — the payload cannot be serialized;</li>
 *   <li>{@link RecordTooLargeException} — the record exceeds the broker limit.</li>
 * </ul>
 * The cause chain is inspected too, so a deterministic exception wrapped by an
 * asynchronous decorator is still recognized.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class DefaultRetryClassifier implements RetryClassifier {

    /**
     * Shared stateless instance.
     */
    public static final DefaultRetryClassifier INSTANCE = new DefaultRetryClassifier();

    private static final int MAX_CAUSE_DEPTH = 16;

    private DefaultRetryClassifier() {
    }

    @Override
    public boolean shouldRetry(Throwable error) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (isDeterministic(current)) {
                return false;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return true;
    }

    private static boolean isDeterministic(Throwable error) {
        return error instanceof EventPublisherConfigException
                || error instanceof IllegalArgumentException
                || error instanceof NullPointerException
                || error instanceof EventSerializationException
                || error instanceof org.apache.kafka.common.errors.SerializationException
                || error instanceof RecordTooLargeException;
    }
}
