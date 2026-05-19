package io.github.vovten.eventflow.dispatcher;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Container for multiple handler results with convenient inspection methods.
 * <p>
 * Use this class when you need to analyze the outcome of dispatching an event
 * to multiple handlers.
 * <p>
 * <b>Example:</b>
 * <pre>{@code
 * HandlerResults results = dispatcher.dispatch(event).join();
 *
 * if (results.isAllSuccess()) {
 *     log.info("All handlers processed the event");
 * } else if (results.isPartialSuccess()) {
 *     log.warn("Partial success. Successful: {}, Failed: {}",
 *         results.getSuccessfulCount(), results.getFailedCount());
 *     results.getFailures().forEach(f -> log.error("Handler {} failed: {}",
 *         f.handlerName(), f.errorDetails()));
 * } else {
 *     log.error("Total failure");
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public final class HandlerResults {

    private final List<HandlerResult> results;
    private final int totalCount;
    private final long successCount;
    private final long failureCount;
    private final boolean allSuccess;
    private final boolean allFailure;
    private final boolean partialSuccess;
    private final String skipReason;

    private HandlerResults(List<HandlerResult> results, String skipReason) {
        this.results = Collections.unmodifiableList(results);
        this.totalCount = results.size();
        this.successCount = results.stream().filter(HandlerResult::success).count();
        this.failureCount = totalCount - successCount;
        this.allSuccess = successCount == totalCount && totalCount > 0;
        this.allFailure = failureCount == totalCount && totalCount > 0;
        this.partialSuccess = !allSuccess && !allFailure && totalCount > 0;
        this.skipReason = skipReason;
    }

    /**
     * Create HandlerResults from a list of HandlerResult.
     *
     * @param results list of handler results
     * @return HandlerResults instance
     */
    public static HandlerResults of(List<HandlerResult> results) {
        return new HandlerResults(results != null ? results : List.of(), null);
    }

    /**
     * Create empty HandlerResults (no handlers found).
     *
     * @return empty HandlerResults
     */
    public static HandlerResults empty() {
        return new HandlerResults(List.of(), "no handlers found");
    }

    /**
     * Create HandlerResults for duplicate event.
     *
     * @return HandlerResults indicating duplicate event
     */
    public static HandlerResults duplicate() {
        return new HandlerResults(List.of(), "duplicate event");
    }

    /**
     * @return true if there are no results (no handlers found or duplicate)
     */
    public boolean isEmpty() {
        return results.isEmpty();
    }

    /**
     * @return the reason for skipping, or null if not skipped
     */
    public String getSkipReason() {
        return skipReason;
    }

    /**
     * @return true if this is a duplicate event
     */
    public boolean isDuplicate() {
        return "duplicate event".equals(skipReason);
    }

    /**
     * @return true if all handlers succeeded (and at least one result exists)
     */
    public boolean isAllSuccess() {
        return allSuccess;
    }

    /**
     * @return true if all handlers failed (and at least one result exists)
     */
    public boolean isAllFailure() {
        return allFailure;
    }

    /**
     * @return true if some handlers succeeded and some failed
     */
    public boolean isPartialSuccess() {
        return partialSuccess;
    }

    /**
     * @return total number of handler execution attempts
     */
    public int getTotalCount() {
        return totalCount;
    }

    /**
     * @return number of successful handler executions
     */
    public long getSuccessfulCount() {
        return successCount;
    }

    /**
     * @return number of failed handler executions
     */
    public long getFailedCount() {
        return failureCount;
    }

    /**
     * @return list of all successful results
     */
    public List<HandlerResult> getSuccesses() {
        return results.stream()
                .filter(HandlerResult::success)
                .toList();
    }

    /**
     * @return list of all failed results
     */
    public List<HandlerResult> getFailures() {
        return results.stream()
                .filter(r -> !r.success())
                .toList();
    }

    /**
     * @return first successful result, or empty if none
     */
    public Optional<HandlerResult> getFirstSuccess() {
        return results.stream()
                .filter(HandlerResult::success)
                .findFirst();
    }

    /**
     * @return first failure, or empty if none
     */
    public Optional<HandlerResult> getFirstFailure() {
        return results.stream()
                .filter(r -> !r.success())
                .findFirst();
    }

    /**
     * @return throwable from first failure, if any
     */
    public Optional<Throwable> getFirstError() {
        return getFirstFailure().flatMap(r -> Optional.ofNullable(r.error()));
    }

    /**
     * @return summary string for logging
     */
    public String getSummary() {
        if (isEmpty()) {
            return "No handlers found";
        }
        if (isAllSuccess()) {
            return String.format("All %d handlers succeeded", totalCount);
        }
        if (isAllFailure()) {
            return String.format("All %d handlers failed", totalCount);
        }
        return String.format("Partial: %d succeeded, %d failed", successCount, failureCount);
    }

    /**
     * @return underlying list of results (for iteration or streaming)
     */
    public List<HandlerResult> asList() {
        return results;
    }

    @Override
    public String toString() {
        return "HandlerResults{" + getSummary() + "}";
    }
}