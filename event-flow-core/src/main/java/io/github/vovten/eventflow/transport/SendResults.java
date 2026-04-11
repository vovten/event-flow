package io.github.vovten.eventflow.transport;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Container for multiple send results with convenient inspection methods.
 * <p>
 * Use this class when you need to analyze the outcome of publishing an event
 * to multiple transports or channels.
 * <p>
 * <b>Example:</b>
 * <pre>{@code
 * SendResults results = publisher.publish(event).join();
 *
 * if (results.isAllSuccess()) {
 *     log.info("All destinations received the event");
 * } else if (results.isPartialSuccess()) {
 *     log.warn("Partial success. Successful: {}, Failed: {}",
 *         results.getSuccessfulCount(), results.getFailedCount());
 *     results.getFailures().forEach(f -> log.error("Failed: {}", f.errorDetails()));
 * } else {
 *     log.error("Total failure: {}", results.getFirstFailure().get().errorDetails());
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-04-10
 */
public final class SendResults {

    private final List<SendResult> results;
    private final int totalCount;
    private final long successCount;
    private final long failureCount;
    private final boolean allSuccess;
    private final boolean allFailure;
    private final boolean partialSuccess;

    private SendResults(List<SendResult> results) {
        this.results = Collections.unmodifiableList(results);
        this.totalCount = results.size();
        this.successCount = results.stream().filter(SendResult::success).count();
        this.failureCount = totalCount - successCount;
        this.allSuccess = successCount == totalCount && totalCount > 0;
        this.allFailure = failureCount == totalCount && totalCount > 0;
        this.partialSuccess = !allSuccess && !allFailure && totalCount > 0;
    }

    /**
     * Create SendResults from a list of SendResult.
     *
     * @param results list of send results
     * @return SendResults instance
     */
    public static SendResults of(List<SendResult> results) {
        return new SendResults(results != null ? results : List.of());
    }

    /**
     * Create empty SendResults (no transports configured).
     *
     * @return empty SendResults
     */
    public static SendResults empty() {
        return new SendResults(List.of());
    }

    /**
     * @return true if there are no results (no transports configured)
     */
    public boolean isEmpty() {
        return results.isEmpty();
    }

    /**
     * @return true if all sends were successful (and at least one result exists)
     */
    public boolean isAllSuccess() {
        return allSuccess;
    }

    /**
     * @return true if all sends failed (and at least one result exists)
     */
    public boolean isAllFailure() {
        return allFailure;
    }

    /**
     * @return true if some sends succeeded and some failed
     */
    public boolean isPartialSuccess() {
        return partialSuccess;
    }

    /**
     * @return total number of send attempts
     */
    public int getTotalCount() {
        return totalCount;
    }

    /**
     * @return number of successful sends
     */
    public long getSuccessfulCount() {
        return successCount;
    }

    /**
     * @return number of failed sends
     */
    public long getFailedCount() {
        return failureCount;
    }

    /**
     * @return list of all successful results
     */
    public List<SendResult> getSuccesses() {
        return results.stream()
                .filter(SendResult::success)
                .toList();
    }

    /**
     * @return list of all failed results
     */
    public List<SendResult> getFailures() {
        return results.stream()
                .filter(r -> !r.success())
                .toList();
    }

    /**
     * @return first successful result, or empty if none
     */
    public Optional<SendResult> getFirstSuccess() {
        return results.stream()
                .filter(SendResult::success)
                .findFirst();
    }

    /**
     * @return first failure, or empty if none
     */
    public Optional<SendResult> getFirstFailure() {
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
            return "No results";
        }
        if (isAllSuccess()) {
            return String.format("All %d succeeded", totalCount);
        }
        if (isAllFailure()) {
            return String.format("All %d failed", totalCount);
        }
        return String.format("Partial: %d succeeded, %d failed", successCount, failureCount);
    }

    /**
     * @return underlying list of results (for iteration or streaming)
     */
    public List<SendResult> asList() {
        return results;
    }

    @Override
    public String toString() {
        return "SendResults{" + getSummary() + "}";
    }
}
