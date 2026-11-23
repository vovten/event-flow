package io.github.vovten.eventflow.lifecycle;

import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;

/**
 * Classifies a failure to reconstruct a stored event for a retry.
 * <p>
 * A malformed payload is permanent: the same bytes can never be deserialized.
 * An unknown type id, a missing class or a linkage failure can be fixed by
 * deploying the missing class or relaxing the deserialization whitelist, so
 * such failures are retried a bounded number of times before the event is
 * dead-lettered.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
final class PayloadFailure {

    /**
     * Default number of attempts for a transient payload failure.
     */
    static final int DEFAULT_RETRY_THRESHOLD = 3;

    private static final int MAX_CAUSE_DEPTH = 16;

    private PayloadFailure() {
    }

    /**
     * Returns whether the failure may disappear after a deployment or a
     * configuration change.
     *
     * @param error the deserialization failure
     * @return true for unknown type ids, missing classes and linkage errors
     */
    static boolean isTransient(Throwable error) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (current instanceof InvalidTypeIdException
                    || current instanceof ClassNotFoundException
                    || current instanceof LinkageError) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return false;
    }

    /**
     * Formats the failure for the retry row diagnostics.
     *
     * @param error the deserialization failure
     * @return the error class name followed by the message
     */
    static String describe(Throwable error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) {
            return error.getClass().getName();
        }
        return error.getClass().getName() + ": " + message;
    }
}
