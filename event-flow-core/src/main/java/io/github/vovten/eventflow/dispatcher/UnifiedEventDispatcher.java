package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.retry.FailureKind;
import io.github.vovten.eventflow.retry.ResolvedRetryPolicy;
import io.github.vovten.eventflow.retry.RetryDelayScheduler;
import io.github.vovten.eventflow.retry.RetryMetadata;
import io.github.vovten.eventflow.transport.InTransport;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.stream.Collectors.joining;

/**
 * Unified event dispatcher that listens to events from multiple transport sources.
 * <p>
 * This dispatcher can listen to multiple
 * {@link InTransport} instances simultaneously, delivering events from all
 * sources to the registered handlers.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Single dispatcher implementation for all transport types</li>
 *   <li>Support for multiple transports (local-queue, Kafka, etc.)</li>
 *   <li>Thread-safe event delivery to handlers</li>
 *   <li>External handler registry injection</li>
 *   <li>Support for decorator pattern via external dispatch consumer injection</li>
 *   <li>Backpressure support via concurrency semaphore (for virtual threads)
 *       or CallerRunsPolicy (for platform thread pools)</li>
 *   <li>Asynchronous local processing retry per handler</li>
 * </ul>
 * <p>
 * <b>Backpressure behavior:</b>
 * <ul>
 *   <li><b>With platform threads + CallerRunsPolicy:</b> When the executor thread pool
 *       is saturated, the transport consumer thread executes the handler directly.
 *       This naturally slows down event polling from transports.</li>
 *   <li><b>With virtual threads + Semaphore:</b> A concurrency semaphore limits the
 *       number of concurrent handler executions. When the semaphore is exhausted,
 *       handler submission blocks until a slot is released, providing controlled
 *       backpressure without overwhelming downstream systems.</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>{@code
 * DispatcherTransport(s) → [IdempotentEventDispatcher] → UnifiedEventDispatcher → EventHandler(s)
 *      ↓ Kafka
 *      ↓ Local-Queue
 *      ↓ Custom...
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class UnifiedEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(UnifiedEventDispatcher.class);

    /**
     * Default absolute cap for local attempts, regardless of the policy value.
     */
    private static final int DEFAULT_LOCAL_RETRY_CAP = 10;

    private final List<InTransport> transports;
    private final ExecutorService executorService;
    private final EventHandlerRegistry handlerRegistry;

    /**
     * Limits concurrent handler executions to prevent overwhelming downstream systems.
     * Null means unlimited (use with platform threads + CallerRunsPolicy).
     * Critical for virtual threads where executor never rejects tasks.
     */
    private final Semaphore concurrencySemaphore;

    /**
     * Non-blocking scheduler used between local retry attempts.
     */
    private final RetryDelayScheduler retryDelayScheduler;

    /**
     * Absolute cap for local attempts, independent of listener policy.
     */
    private final int localRetryCap;

    /**
     * Fallback policy for handlers that do not expose one.
     */
    private final ResolvedRetryPolicy defaultRetryPolicy = ResolvedRetryPolicy.defaults();

    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Create unified dispatcher with custom handler registry.
     * <p>
     * <b>Resource ownership:</b> This dispatcher does NOT close the provided
     * {@code executorService}. The caller is responsible for shutting down the executor
     * when it is no longer needed. This follows the standard dependency injection principle
     * where the component that creates a resource is responsible for its lifecycle.
     *
     * @param executorService executor service for async handler execution (NOT closed by dispatcher)
     * @param handlerRegistry custom handler registry
     * @param transports      list of transports to listen to
     */
    public UnifiedEventDispatcher(ExecutorService executorService,
                                  EventHandlerRegistry handlerRegistry,
                                  List<InTransport> transports) {
        this(executorService, handlerRegistry, transports, null,
                RetryDelayScheduler.defaultScheduler(), DEFAULT_LOCAL_RETRY_CAP);
    }

    /**
     * Create unified dispatcher with concurrency limiting for backpressure support.
     *
     * @param executorService      executor service for async handler execution (NOT closed by dispatcher)
     * @param handlerRegistry      custom handler registry
     * @param transports           list of transports to listen to
     * @param concurrencySemaphore semaphore for limiting concurrent handler executions (null = unlimited)
     */
    public UnifiedEventDispatcher(ExecutorService executorService,
                                  EventHandlerRegistry handlerRegistry,
                                  List<InTransport> transports,
                                  Semaphore concurrencySemaphore) {
        this(executorService, handlerRegistry, transports, concurrencySemaphore,
                RetryDelayScheduler.defaultScheduler(), DEFAULT_LOCAL_RETRY_CAP);
    }

    /**
     * Create unified dispatcher with a custom retry delay scheduler.
     *
     * @param executorService      executor service for async handler execution (NOT closed by dispatcher)
     * @param handlerRegistry      custom handler registry
     * @param transports           list of transports to listen to
     * @param concurrencySemaphore semaphore for limiting concurrent handler executions (null = unlimited)
     * @param retryDelayScheduler  non-blocking delay scheduler for local retries
     */
    public UnifiedEventDispatcher(ExecutorService executorService,
                                  EventHandlerRegistry handlerRegistry,
                                  List<InTransport> transports,
                                  Semaphore concurrencySemaphore,
                                  RetryDelayScheduler retryDelayScheduler) {
        this(executorService, handlerRegistry, transports, concurrencySemaphore,
                retryDelayScheduler, DEFAULT_LOCAL_RETRY_CAP);
    }

    /**
     * Create unified dispatcher with full local retry configuration.
     *
     * @param executorService      executor service for async handler execution (NOT closed by dispatcher)
     * @param handlerRegistry      custom handler registry
     * @param transports           list of transports to listen to
     * @param concurrencySemaphore semaphore for limiting concurrent handler executions (null = unlimited)
     * @param retryDelayScheduler  non-blocking delay scheduler for local retries
     * @param localRetryCap        absolute cap for local attempts (must be positive)
     */
    public UnifiedEventDispatcher(ExecutorService executorService,
                                  EventHandlerRegistry handlerRegistry,
                                  List<InTransport> transports,
                                  Semaphore concurrencySemaphore,
                                  RetryDelayScheduler retryDelayScheduler,
                                  int localRetryCap) {
        if (localRetryCap <= 0) {
            throw new IllegalArgumentException("localRetryCap must be positive");
        }
        this.transports = List.copyOf(transports);
        this.executorService = executorService;
        this.handlerRegistry = handlerRegistry;
        this.concurrencySemaphore = concurrencySemaphore;
        this.retryDelayScheduler = retryDelayScheduler;
        this.localRetryCap = localRetryCap;
    }

    @Override
    public void start(Consumer<Event> dispatchConsumer) {
        if (started.compareAndSet(false, true)) {
            for (InTransport transport : transports) {
                transport.start(dispatchConsumer);
            }
            log.info(buildDispatcherStartedMsg());
        } else {
            log.warn("UnifiedEventDispatcher is already started");
        }
    }

    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            log.info("Stopping UnifiedEventDispatcher...");
            for (InTransport transport : transports) {
                tryStop(transport);
            }
            log.info("UnifiedEventDispatcher stopped");
        }
    }

    @Override
    public CompletableFuture<HandlerResults> dispatch(Event event) {
        List<EventHandler> handlers = handlerRegistry.getHandlers(event);
        if (handlers.isEmpty()) {
            log.debug("No handlers found for event: {}", event);
            return CompletableFuture.completedFuture(HandlerResults.empty());
        }

        int attempt = RetryMetadata.attempt(event);
        List<CompletableFuture<HandlerResult>> futures = new ArrayList<>();

        for (EventHandler handler : handlers) {
            try {
                futures.add(executeHandlerWithRetry(handler, event, attempt));
            } catch (Exception e) {
                log.warn("Failed to submit handler {}: {}", handler.name(), e.getMessage());
                futures.add(CompletableFuture.completedFuture(
                        HandlerResult.failure(handler.name(), e)));
            }
        }

        CompletableFuture<Void> allFutures = CompletableFuture
                .allOf(futures.toArray(CompletableFuture[]::new));

        return allFutures.thenApply(v ->
                HandlerResults.of(futures.stream()
                        .map(CompletableFuture::join)
                        .toList()));
    }

    private CompletableFuture<HandlerResult> executeHandlerWithRetry(EventHandler handler, Event event, int attempt) {
        ResolvedRetryPolicy policy = resolvePolicy(handler);
        return invokeOnce(handler, event)
                .thenCompose(outcome -> {
                    if (outcome.success()) {
                        return CompletableFuture.completedFuture(
                                HandlerResult.success(handler.name(), attempt));
                    }
                    return handleFailure(handler, event, attempt, policy, outcome.error());
                });
    }

    private CompletableFuture<AttemptOutcome> invokeOnce(EventHandler handler, Event event) {
        try {
            return CompletableFuture.supplyAsync(() -> invokeHandler(handler, event), executorService);
        } catch (RejectedExecutionException e) {
            log.warn("Failed to submit handler {}: executor rejected task", handler.name(), e);
            return CompletableFuture.completedFuture(AttemptOutcome.failed(e));
        }
    }

    private AttemptOutcome invokeHandler(EventHandler handler, Event event) {
        boolean acquired = false;
        if (concurrencySemaphore != null) {
            try {
                concurrencySemaphore.acquire();
                acquired = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return AttemptOutcome.failed(e);
            }
        }
        try {
            handler.onEvent(event);
            return AttemptOutcome.succeeded();
        } catch (Exception e) {
            return AttemptOutcome.failed(e);
        } finally {
            if (acquired) {
                concurrencySemaphore.release();
            }
        }
    }

    private CompletableFuture<HandlerResult> handleFailure(EventHandler handler, Event event, int attempt,
                                                           ResolvedRetryPolicy policy, Throwable error) {
        if (error instanceof InterruptedException) {
            return CompletableFuture.completedFuture(failure(
                    handler, error, attempt, policy, FailureKind.TRANSIENT_EXHAUSTED, true, 0L));
        }
        if (!policy.matches(error)) {
            log.error("Handler {} failed permanently for event {} on attempt {}: {}",
                    handler.name(), event.type().getSimpleName(), attempt, errorDetails(error));
            return CompletableFuture.completedFuture(failure(
                    handler, error, attempt, policy, FailureKind.PERMANENT, false, 0L));
        }
        if (!policy.hasMoreAttempts(attempt)) {
            log.error("Handler {} exhausted the retry budget for event {} on attempt {}",
                    handler.name(), event.type().getSimpleName(), attempt);
            return CompletableFuture.completedFuture(failure(
                    handler, error, attempt, policy, FailureKind.TRANSIENT_EXHAUSTED, false, 0L));
        }
        if (policy.isLocalRetryAllowed(attempt) && attempt < localRetryCap) {
            Duration delay = policy.backoff(attempt);
            log.warn("Handler {} failed on attempt {}/{} for event {}; retrying in {}ms",
                    handler.name(), attempt, policy.maxRetries(), event.type().getSimpleName(),
                    delay.toMillis());
            return retryDelayScheduler.delay(delay)
                    .thenCompose(ignored -> executeHandlerWithRetry(handler, event, attempt + 1));
        }
        long retryAfterMs = policy.backoff(attempt).toMillis();
        log.warn("Handler {} failed on attempt {} for event {}; handing off to durable retry after {}ms",
                handler.name(), attempt, event.type().getSimpleName(), retryAfterMs);
        return CompletableFuture.completedFuture(failure(
                handler, error, attempt, policy, FailureKind.TRANSIENT_EXHAUSTED, true, retryAfterMs));
    }

    private HandlerResult failure(EventHandler handler, Throwable error, int attempt,
                                  ResolvedRetryPolicy policy, FailureKind kind,
                                  boolean retryable, long retryAfterMs) {
        return HandlerResult.failure(handler.name(), error, errorDetails(error),
                kind, retryable, attempt, policy.name(), retryAfterMs);
    }

    private ResolvedRetryPolicy resolvePolicy(EventHandler handler) {
        ResolvedRetryPolicy policy = handlerRegistry.retryPolicyFor(handler);
        return policy != null ? policy : defaultRetryPolicy;
    }

    private static String errorDetails(Throwable error) {
        return error != null ? ExceptionUtils.getRootCauseMessage(error) : null;
    }

    @Override
    public void register(Object handler) {
        if (!isRegistered(handler)) {
            handlerRegistry.register(handler);
        }
    }

    @Override
    public boolean isRegistered(Object handler) {
        return handlerRegistry.isRegistered(handler);
    }

    private void tryStop(InTransport transport) {
        try {
            transport.stop();
        } catch (Exception e) {
            log.warn("Error stopping transport {}", transport.name(), e);
        }
    }

    private String buildDispatcherStartedMsg() {
        String msg = "EventDispatcher started with %s transport(s): %s";
        String names = transports.stream().map(InTransport::name).collect(joining(","));
        return String.format(msg, transports.size(), names);
    }

    private record AttemptOutcome(boolean success, Throwable error) {

        private static AttemptOutcome succeeded() {
            return new AttemptOutcome(true, null);
        }

        private static AttemptOutcome failed(Throwable error) {
            return new AttemptOutcome(false, error);
        }
    }
}
