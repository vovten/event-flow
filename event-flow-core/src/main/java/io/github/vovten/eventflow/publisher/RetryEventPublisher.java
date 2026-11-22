package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Decorator for {@link EventPublisher} that adds automatic retry support for failed event publishing.
 * <p>
 * When event publishing fails, this decorator will automatically retry the operation with
 * exponential backoff delay between attempts. This helps handle transient failures such as
 * network issues, temporary broker unavailability, or connection timeouts.
 * <p>
 * <b>Retry strategy:</b>
 * <ul>
 *   <li>Uses exponential backoff: delay = initialDelay * (multiplier ^ (attempt - 1)),
 *       capped by {@code maxDelay} and optionally jittered by ±{@code jitter}</li>
 *   <li>Retries every failure except {@link EventPublisherConfigException} and
 *       {@link IllegalArgumentException}, which are treated as configuration or
 *       programmer errors. This includes NPEs and serialization failures: the
 *       decorator is best-effort and leaves the retry budget to bound them</li>
 *   <li>Also retries when the delegate reports that <b>all</b> destinations failed
 *       (e.g. a full local queue or a rejected transport), because no durable
 *       delivery happened for any destination</li>
 *   <li>Does not retry partial success: some destinations already received the
 *       event, so a retry would duplicate delivery</li>
 *   <li>Logs each retry attempt with delay information</li>
 *   <li>After all retries exhausted: exceptions complete the future exceptionally;
 *       all-failure results complete it with the last failed {@link SendResults}</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * EventPublisher basePublisher = new ChannelEventPublisher(channels);
 * EventPublisher retryPublisher = new RetryEventPublisher(
 *     basePublisher,
 *     3,                              // max 3 retries
 *     Duration.ofMillis(100),         // initial delay 100ms
 *     2                               // multiplier 2x (100ms → 200ms → 400ms)
 * );
 * retryPublisher.publish(event)
 *     .thenAccept(results -> log.info("Published to {} destinations", results.size()))
 *     .exceptionally(ex -> { log.error("Failed after retries", ex); return null; });
 * }</pre>
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Unreliable network connections</li>
 *   <li>Message brokers that may have temporary outages</li>
 *   <li>Critical events that must be delivered</li>
 * </ul>
 * <p>
 * <b>When NOT to use:</b>
 * <ul>
 *   <li>High-throughput scenarios where latency is critical</li>
 *   <li>Events that can be safely lost</li>
 *   <li>When using transactional outbox pattern instead</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @see ChannelEventPublisher
 * @since 1.0.0
 */
public class RetryEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RetryEventPublisher.class);

    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(10);
    private static final double DEFAULT_JITTER = 0.0;

    /**
     * Dedicated scheduler for retry delays. The common ForkJoinPool is shared
     * with unrelated work and can delay or starve these timers.
     */
    private static final ScheduledExecutorService DELAYER =
            Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "retry-publisher-delayer");
                thread.setDaemon(true);
                return thread;
            });

    private final EventPublisher origin;
    private final int maxRetries;
    private final Duration initialDelay;
    private final double multiplier;
    private final Duration maxDelay;
    private final double jitter;

    /**
     * Create retry decorator with default settings.
     * <ul>
     *   <li>Max retries: 3</li>
     *   <li>Initial delay: 100ms</li>
     *   <li>Multiplier: 2.0 (exponential backoff)</li>
     *   <li>Max delay: 10s</li>
     * </ul>
     *
     * @param origin the delegate publisher to wrap
     */
    public RetryEventPublisher(EventPublisher origin) {
        this(origin, 3, Duration.ofMillis(100), 2.0, DEFAULT_MAX_DELAY, DEFAULT_JITTER);
    }

    /**
     * Create retry decorator with custom settings.
     *
     * @param origin       the origin publisher to wrap
     * @param maxRetries   maximum number of retry attempts (must be &gt;= 0)
     * @param initialDelay initial delay between retries
     * @param multiplier   backoff multiplier (must be &gt;= 1.0)
     * @throws IllegalArgumentException if maxRetries &lt; 0 or multiplier &lt; 1.0
     */
    public RetryEventPublisher(EventPublisher origin, int maxRetries, Duration initialDelay, double multiplier) {
        this(origin, maxRetries, initialDelay, multiplier, DEFAULT_MAX_DELAY, DEFAULT_JITTER);
    }

    /**
     * Create retry decorator with custom settings.
     *
     * @param origin       the origin publisher to wrap
     * @param maxRetries   maximum number of retry attempts (must be &gt;= 0)
     * @param initialDelay initial delay between retries
     * @param multiplier   backoff multiplier (must be &gt;= 1.0)
     * @param maxDelay     maximum delay between retries (caps exponential backoff)
     * @throws IllegalArgumentException if maxRetries &lt; 0, multiplier &lt; 1.0, or maxDelay is not positive
     */
    public RetryEventPublisher(EventPublisher origin, int maxRetries, Duration initialDelay,
                               double multiplier, Duration maxDelay) {
        this(origin, maxRetries, initialDelay, multiplier, maxDelay, DEFAULT_JITTER);
    }

    /**
     * Create retry decorator with custom settings, including jitter.
     *
     * @param origin       the origin publisher to wrap
     * @param maxRetries   maximum number of retry attempts (must be &gt;= 0)
     * @param initialDelay initial delay between retries
     * @param multiplier   backoff multiplier (must be &gt;= 1.0)
     * @param maxDelay     maximum delay between retries (caps exponential backoff)
     * @param jitter       symmetric jitter fraction in {@code [0, 1]}; {@code 0}
     *                     disables jitter
     * @throws IllegalArgumentException if maxRetries &lt; 0, multiplier &lt; 1.0,
     *                                  maxDelay is not positive, or jitter is outside {@code [0, 1]}
     */
    public RetryEventPublisher(EventPublisher origin, int maxRetries, Duration initialDelay,
                               double multiplier, Duration maxDelay, double jitter) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("Max retries must be >= 0");
        }
        if (initialDelay.isNegative() || initialDelay.isZero()) {
            throw new IllegalArgumentException("Initial delay must be positive");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("Multiplier must be >= 1.0");
        }
        if (maxDelay.isNegative() || maxDelay.isZero()) {
            throw new IllegalArgumentException("Max delay must be positive");
        }
        if (jitter < 0.0 || jitter > 1.0) {
            throw new IllegalArgumentException("Jitter must be between 0 and 1");
        }
        this.origin = origin;
        this.maxRetries = maxRetries;
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
        this.maxDelay = maxDelay;
        this.jitter = jitter;
    }

    @Override
    public CompletableFuture<SendResults> publish(Event event) {
        CompletableFuture<SendResults> resultFuture = new CompletableFuture<>();
        retryPublish(event, 1, resultFuture);
        return resultFuture;
    }

    private void retryPublish(Event event, int attempt, CompletableFuture<SendResults> resultFuture) {
        String eventTypeName = event.type().getSimpleName();
        CompletableFuture<SendResults> originFuture;
        try {
            originFuture = origin.publish(event);
        } catch (RuntimeException e) {
            // The EventPublisher contract promises an asynchronous result, but a
            // misbehaving delegate may still throw synchronously. Treat it like
            // any other failure instead of leaking the exception to the caller.
            handleError(event, attempt, resultFuture, eventTypeName, e);
            return;
        }
        if (originFuture == null) {
            handleError(event, attempt, resultFuture, eventTypeName,
                    new NullPointerException("origin.publish returned a null future"));
            return;
        }
        originFuture.whenComplete((results, error) -> {
            if (error != null) {
                handleError(event, attempt, resultFuture, eventTypeName, error);
                return;
            }
            if (results != null && results.isAllFailure()) {
                // Transport-level rejection reported as a result (for example a full
                // local queue): nothing was delivered, so retrying is safe.
                String details = results.getFirstFailure()
                        .map(SendResult::errorDetails)
                        .orElse("all destinations failed");
                if (scheduleRetryIfAllowed(event, attempt, resultFuture, eventTypeName, details)) {
                    return;
                }
                log.error("Failed to publish event {} after {} attempts: {}",
                        eventTypeName, maxRetries + 1, details);
                resultFuture.complete(results);
                return;
            }
            resultFuture.complete(results);
        });
    }

    private void handleError(Event event, int attempt, CompletableFuture<SendResults> resultFuture,
                             String eventTypeName, Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null
                ? error.getCause()
                : error;
        if (!shouldRetry(cause)) {
            log.warn("Non-retryable error publishing event {}: {}", eventTypeName, cause.getMessage());
            resultFuture.completeExceptionally(error);
            return;
        }
        if (scheduleRetryIfAllowed(event, attempt, resultFuture, eventTypeName, cause.getMessage())) {
            return;
        }
        log.error("Failed to publish event {} after {} attempts", eventTypeName, maxRetries + 1, cause);
        String msg = String.format("Failed to publish event %s after %d attempts", eventTypeName, maxRetries + 1);
        resultFuture.completeExceptionally(new EventPublisherException(msg, cause));
    }

    private boolean scheduleRetryIfAllowed(Event event, int attempt, CompletableFuture<SendResults> resultFuture,
                                           String eventTypeName, String reason) {
        if (attempt > maxRetries) {
            return false;
        }
        long delayMs = calculateDelay(attempt);
        log.warn("Failed to publish event {} (attempt {}/{}). Retrying in {}ms: {}",
                eventTypeName, attempt, maxRetries + 1, delayMs, reason);
        scheduleRetry(event, attempt, delayMs, resultFuture);
        return true;
    }

    private void scheduleRetry(Event event, int attempt, long delayMs, CompletableFuture<SendResults> resultFuture) {
        DELAYER.schedule(() -> retryPublish(event, attempt + 1, resultFuture),
                delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Calculate delay for the given attempt using exponential backoff, capped
     * at {@code maxDelay} and optionally jittered.
     *
     * @param attempt the current attempt number (1-based)
     * @return delay in milliseconds
     */
    private long calculateDelay(int attempt) {
        double delay = initialDelay.toMillis() * Math.pow(multiplier, attempt - 1);
        long capped = (long) Math.min(delay, maxDelay.toMillis());
        return applyJitter(capped);
    }

    private long applyJitter(long delayMillis) {
        if (jitter <= 0.0) {
            return delayMillis;
        }
        long bound = Math.max(1L, (long) (delayMillis * jitter));
        long offset = ThreadLocalRandom.current().nextLong(-bound, bound + 1);
        return Math.max(0L, delayMillis + offset);
    }

    /**
     * Determine if the exception is retryable.
     * <p>
     * Retryable exceptions include:
     * <ul>
     *   <li>Network exceptions</li>
     *   <li>Timeout exceptions</li>
     *   <li>Temporary broker unavailability</li>
     * </ul>
     * <p>
     * Non-retryable exceptions include:
     * <ul>
     *   <li>{@link EventPublisherConfigException} - configuration errors</li>
     *   <li>{@link IllegalArgumentException} - invalid arguments</li>
     * </ul>
     *
     * @param ex the exception to check
     * @return true if the exception should trigger a retry
     */
    private boolean shouldRetry(Throwable ex) {
        if (ex instanceof EventPublisherConfigException) {
            return false;
        }
        if (ex instanceof IllegalArgumentException) {
            return false;
        }
        return true;
    }
}
