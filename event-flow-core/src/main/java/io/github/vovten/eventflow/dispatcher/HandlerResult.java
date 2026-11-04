package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.retry.FailureKind;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Objects;

/**
 * Result of a handler execution.
 *
 * @param success       true if handler executed successfully
 * @param handlerName   name of the handler
 * @param error         exception if handler failed, null otherwise
 * @param errorDetails  error message if handler failed, null otherwise
 * @param failureKind   processing-retry classification
 * @param retryable     whether another processing attempt is allowed
 * @param attempts      number of attempts performed by this handler
 * @param policyName    effective retry policy name, or null when unknown
 * @param retryAfterMs  relative delay before the next attempt, or 0 when not applicable
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public record HandlerResult(
        boolean success,
        String handlerName,
        Throwable error,
        String errorDetails,
        FailureKind failureKind,
        boolean retryable,
        int attempts,
        String policyName,
        long retryAfterMs
) {

    public HandlerResult {
        Objects.requireNonNull(handlerName, "handlerName must not be null");
        Objects.requireNonNull(failureKind, "failureKind must not be null");
        if (attempts < 1) {
            throw new IllegalArgumentException("attempts must be >= 1");
        }
        if (retryAfterMs < 0) {
            throw new IllegalArgumentException("retryAfterMs must not be negative");
        }
    }

    /**
     * Legacy constructor retained for backward compatibility.
     * <p>
     * A legacy failure is treated as retryable with an exhausted local budget so
     * that existing durable-retry behaviour is preserved.
     *
     * @param success      true if handler executed successfully
     * @param handlerName  name of the handler
     * @param error        exception if handler failed, null otherwise
     * @param errorDetails error message if handler failed, null otherwise
     */
    public HandlerResult(boolean success, String handlerName, Throwable error, String errorDetails) {
        this(success, handlerName, error, errorDetails,
                success ? FailureKind.NONE : FailureKind.TRANSIENT_EXHAUSTED,
                !success, 1, null, 0L);
    }

    /**
     * Creates a successful handler result with one attempt.
     *
     * @param handlerName name of the handler
     * @return successful HandlerResult
     */
    public static HandlerResult success(String handlerName) {
        return success(handlerName, 1);
    }

    /**
     * Creates a successful handler result with the given attempt count.
     *
     * @param handlerName name of the handler
     * @param attempts    number of attempts performed
     * @return successful HandlerResult
     */
    public static HandlerResult success(String handlerName, int attempts) {
        return new HandlerResult(true, handlerName, null, null,
                FailureKind.NONE, false, attempts, null, 0L);
    }

    /**
     * Creates a legacy failed handler result. The failure is retryable and has
     * no explicit policy context.
     *
     * @param handlerName name of the handler
     * @param error       exception that caused the failure
     * @return failed HandlerResult
     */
    public static HandlerResult failure(String handlerName, Throwable error) {
        return failure(handlerName, error, resolveMessage(error),
                FailureKind.TRANSIENT_EXHAUSTED, true, 1, null, 0L);
    }

    /**
     * Creates a legacy failed handler result without an exception.
     *
     * @param handlerName  name of the handler
     * @param errorDetails error message
     * @return failed HandlerResult
     */
    public static HandlerResult failure(String handlerName, String errorDetails) {
        return failure(handlerName, null, errorDetails,
                FailureKind.TRANSIENT_EXHAUSTED, true, 1, null, 0L);
    }

    /**
     * Creates a classified failed handler result.
     *
     * @param handlerName  name of the handler
     * @param error        exception that caused the failure, may be null
     * @param errorDetails error message, may be null
     * @param failureKind  processing-retry classification
     * @param retryable    whether another processing attempt is allowed
     * @param attempts     number of attempts performed
     * @param policyName   effective policy name, may be null
     * @param retryAfterMs relative delay before the next attempt, or 0
     * @return failed HandlerResult
     */
    public static HandlerResult failure(String handlerName, Throwable error, String errorDetails,
                                        FailureKind failureKind, boolean retryable, int attempts,
                                        String policyName, long retryAfterMs) {
        return new HandlerResult(false, handlerName, error, errorDetails,
                failureKind, retryable, attempts, policyName, retryAfterMs);
    }

    private static String resolveMessage(Throwable error) {
        if (error == null) {
            return null;
        }
        Throwable rootCause = ExceptionUtils.getRootCause(error);
        if (rootCause != null && rootCause.getMessage() != null) {
            return rootCause.getMessage();
        }
        return error.getMessage();
    }
}
