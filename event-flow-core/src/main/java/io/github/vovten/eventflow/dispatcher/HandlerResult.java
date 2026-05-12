package io.github.vovten.eventflow.dispatcher;

/**
 * Result of a handler execution.
 *
 * @param success       true if handler executed successfully
 * @param handlerName   name of the handler
 * @param error         exception if handler failed, null otherwise
 * @param errorDetails  error message if handler failed, null otherwise
 *
 * @author Vladimir Aleshkov
 * @since 2026-05-11
 */
public record HandlerResult(
        boolean success,
        String handlerName,
        Throwable error,
        String errorDetails
) {

    /**
     * Creates a successful handler result.
     *
     * @param handlerName name of the handler
     * @return successful HandlerResult
     */
    public static HandlerResult success(String handlerName) {
        return new HandlerResult(true, handlerName, null, null);
    }

    /**
     * Creates a failed handler result.
     *
     * @param handlerName name of the handler
     * @param error       exception that caused the failure
     * @return failed HandlerResult
     */
    public static HandlerResult failure(String handlerName, Throwable error) {
        return new HandlerResult(false, handlerName, error, error.getMessage());
    }

    /**
     * Creates a failed handler result without exception.
     *
     * @param handlerName   name of the handler
     * @param errorDetails  error message
     * @return failed HandlerResult
     */
    public static HandlerResult failure(String handlerName, String errorDetails) {
        return new HandlerResult(false, handlerName, null, errorDetails);
    }
}