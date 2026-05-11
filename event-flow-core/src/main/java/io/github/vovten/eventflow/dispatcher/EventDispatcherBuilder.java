package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.transport.InTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * Fluent builder for creating configured {@link EventDispatcher} instances.
 * <p>
 * Allows flexible composition of different dispatcher features:
 * <ul>
 *   <li>Unified dispatcher with multiple transports</li>
 *   <li>Idempotent event processing</li>
 *   <li>Custom decorators</li>
 * </ul>
 * <p>
 * <b>Usage examples:</b>
 * <pre>{@code
 * // Simple dispatcher without decorators
 * EventDispatcher dispatcher = EventDispatcherBuilder.create()
 *     .executor(executorService)
 *     .handlerRegistry(registry)
 *     .transports(transports)
 *     .build();
 * dispatcher.start(dispatcher::dispatch);
 *
 * // Dispatcher with concurrency limit (for virtual threads)
 * EventDispatcher dispatcher = EventDispatcherBuilder.create()
 *     .executor(Executors.newVirtualThreadPerTaskExecutor())
 *     .handlerRegistry(registry)
 *     .transports(transports)
 *     .concurrencyLimit(50)
 *     .build();
 * dispatcher.start(dispatcher::dispatch);
 *
 * // Dispatcher with idempotent processing (default settings)
 * EventDispatcher dispatcher = EventDispatcherBuilder.create()
 *     .executor(executorService)
 *     .handlerRegistry(registry)
 *     .transports(transports)
 *     .idempotent()
 *     .build();
 * dispatcher.start(dispatcher::dispatch);
 *
 * // Dispatcher with custom idempotent settings
 * EventDispatcher dispatcher = EventDispatcherBuilder.create()
 *     .executor(executorService)
 *     .handlerRegistry(registry)
 *     .transports(transports)
 *     .idempotent(Duration.ofMinutes(30), 50_000, true)
 *     .build();
 * dispatcher.start(dispatcher::dispatch);
 *
 * // Dispatcher with custom decorator
 * EventDispatcher dispatcher = EventDispatcherBuilder.create()
 *     .executor(executorService)
 *     .handlerRegistry(registry)
 *     .transports(transports)
 *     .withDecorator(dispatcher -> new MetricsEventDispatcher(dispatcher, metricsRegistry))
 *     .build();
 * dispatcher.start(dispatcher::dispatch);
 * }</pre>
 * <p>
 * <b>Order of decorators:</b>
 * The builder applies decorators in the following order (from innermost to outermost):
 * <ol>
 *   <li>Base {@link UnifiedEventDispatcher}</li>
 *   <li>Custom decorators (applied in order added)</li>
 *   <li>{@link IdempotentEventDispatcher} (if enabled)</li>
 * </ol>
 * <p>
 * <b>Event flow:</b>
 * <pre>{@code
 * Transport → IdempotentEventDispatcher → [CustomDecorators] → UnifiedEventDispatcher → Handlers
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
public final class EventDispatcherBuilder {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcherBuilder.class);

    private ExecutorService executorService;
    private EventHandlerRegistry handlerRegistry;
    private final List<InTransport> transports = new ArrayList<>();
    private Integer concurrencyLimit = null;
    private boolean idempotent = false;
    private Duration idempotentTtl = Duration.ofMinutes(10);
    private long idempotentMaxSize = 10_000;
    private boolean idempotentWarnOnDuplicate = true;
    private boolean loggable = false;
    private int logMaxPayloadLength = 500;
    private final List<DecoratorFunction> decorators = new ArrayList<>();

    private EventDispatcherBuilder() {
    }

    /**
     * Start building a new EventDispatcher.
     *
     * @return builder instance
     */
    public static EventDispatcherBuilder create() {
        return new EventDispatcherBuilder();
    }

    /**
     * Set the executor service for async handler execution.
     *
     * @param executorService executor service
     * @return this builder
     */
    public EventDispatcherBuilder executor(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Set the handler registry.
     *
     * @param handlerRegistry handler registry
     * @return this builder
     */
    public EventDispatcherBuilder handlerRegistry(EventHandlerRegistry handlerRegistry) {
        this.handlerRegistry = handlerRegistry;
        return this;
    }

    /**
     * Set the list of transports.
     *
     * @param transports list of transports
     * @return this builder
     */
    public EventDispatcherBuilder transports(List<InTransport> transports) {
        this.transports.clear();
        this.transports.addAll(transports);
        return this;
    }

    /**
     * Add transports to the configuration.
     *
     * @param transports transports to add
     * @return this builder
     */
    public EventDispatcherBuilder addTransports(List<InTransport> transports) {
        this.transports.addAll(transports);
        return this;
    }

    /**
     * Add transports to the configuration.
     *
     * @param transports transports to add
     * @return this builder
     */
    public EventDispatcherBuilder addTransports(InTransport... transports) {
        this.transports.addAll(List.of(transports));
        return this;
    }

    /**
     * Set concurrency limit for handler execution.
     * <p>
     * When using virtual threads, the executor never rejects tasks. Without a limit,
     * a burst of events can spawn thousands of concurrent handler executions,
     * potentially overwhelming downstream systems (databases, HTTP services).
     * <p>
     * The concurrency limit acts as a semaphore: when all slots are occupied,
     * handler submission blocks until a slot is released. This provides
     * backpressure similar to CallerRunsPolicy but works with virtual threads.
     * <p>
     * <b>When to use:</b>
     * <ul>
     *   <li>Virtual thread executors with I/O-bound handlers</li>
     *   <li>Protecting downstream systems from overload</li>
     *   <li>Controlling memory usage during event bursts</li>
     * </ul>
     * <p>
     * <b>Recommended values:</b>
     * <ul>
     *   <li>I/O-bound handlers (DB, HTTP): 20-100</li>
     *   <li>CPU-bound handlers: number of CPU cores</li>
     *   <li>Mixed workload: 50-200</li>
     * </ul>
     *
     * @param maxConcurrent maximum number of concurrent handler executions
     * @return this builder
     * @throws IllegalArgumentException if maxConcurrent &lt;= 0
     */
    public EventDispatcherBuilder concurrencyLimit(int maxConcurrent) {
        if (maxConcurrent <= 0) {
            throw new IllegalArgumentException("Concurrency limit must be positive: " + maxConcurrent);
        }
        this.concurrencyLimit = maxConcurrent;
        return this;
    }

    /**
     * Enable idempotent event processing with default settings:
     * <ul>
     *   <li>TTL: 10 minutes</li>
     *   <li>Max size: 10,000 entries</li>
     *   <li>Warn on duplicate: true</li>
     * </ul>
     *
     * @return this builder
     */
    public EventDispatcherBuilder idempotent() {
        this.idempotent = true;
        return this;
    }

    /**
     * Enable idempotent event processing with custom settings.
     *
     * @param ttl time-to-live for cached event UIDs
     * @param maxSize maximum number of entries in the cache
     * @param warnOnDuplicate whether to log warnings on duplicate events
     * @return this builder
     */
    public EventDispatcherBuilder idempotent(Duration ttl, long maxSize, boolean warnOnDuplicate) {
        this.idempotent = true;
        this.idempotentTtl = ttl;
        this.idempotentMaxSize = maxSize;
        this.idempotentWarnOnDuplicate = warnOnDuplicate;
        return this;
    }

    /**
     * Add a custom decorator to the dispatcher chain.
     * Decorators are applied in the order they are added.
     *
     * @param decorator function that transforms an EventDispatcher into a decorated one
     * @return this builder
     */
    public EventDispatcherBuilder withDecorator(DecoratorFunction decorator) {
        this.decorators.add(decorator);
        return this;
    }

    /**
     * Enable logging decorator with default max payload length (500).
     *
     * @return this builder
     */
    public EventDispatcherBuilder loggable() {
        this.loggable = true;
        return this;
    }

    /**
     * Enable logging decorator with custom max payload length.
     *
     * @param maxPayloadLength maximum length of payload in log output
     * @return this builder
     */
    public EventDispatcherBuilder loggable(int maxPayloadLength) {
        this.loggable = true;
        this.logMaxPayloadLength = maxPayloadLength;
        return this;
    }

    /**
     * Build the EventDispatcher instance with all configured features.
     *
     * @return configured EventDispatcher
     * @throws IllegalStateException if required parameters are not set
     */
    public EventDispatcher build() {
        if (executorService == null) {
            throw new IllegalStateException("ExecutorService must be configured");
        }
        if (handlerRegistry == null) {
            throw new IllegalStateException("EventHandlerRegistry must be configured");
        }
        Semaphore semaphore = concurrencyLimit != null ? new Semaphore(concurrencyLimit) : null;
        EventDispatcher dispatcher = new UnifiedEventDispatcher(
                executorService, handlerRegistry, transports, semaphore
        );
        if (loggable) {
            dispatcher = new LoggingEventDispatcher(dispatcher, logMaxPayloadLength);
            log.debug("Applied logging decorator with maxPayloadLength={}", logMaxPayloadLength);
        }
        // Apply custom decorators
        for (DecoratorFunction decorator : decorators) {
            dispatcher = decorator.apply(dispatcher);
            log.debug("Applied custom decorator: {}", decorator.getClass().getSimpleName());
        }
        if (idempotent) {
            dispatcher = new IdempotentEventDispatcher(
                    dispatcher,
                    idempotentTtl,
                    idempotentMaxSize,
                    idempotentWarnOnDuplicate
            );
            log.debug("Applied idempotent decorator with ttl={}, maxSize={}, warnOnDuplicate={}",
                    idempotentTtl, idempotentMaxSize, idempotentWarnOnDuplicate);
        }
        if (concurrencyLimit != null) {
            log.info("Concurrency limit applied: max {} concurrent handler executions", concurrencyLimit);
        }
        return dispatcher;
    }

    /**
     * Build and return the dispatcher, logging the final configuration.
     *
     * @return configured EventDispatcher
     */
    public EventDispatcher buildAndLog() {
        EventDispatcher dispatcher = build();
        String concurrencyInfo = concurrencyLimit != null ? "limit=" + concurrencyLimit : "unlimited";
        log.info("Built EventDispatcher with configuration: transports={}, idempotent={}, logging={}, customDecorators={}, concurrency={}",
                transports.size(),
                idempotent ? "enabled" : "disabled",
                loggable ? "enabled" : "disabled",
                decorators.size(),
                concurrencyInfo
        );
        return dispatcher;
    }

    /**
     * Functional interface for custom decorators.
     */
    @FunctionalInterface
    public interface DecoratorFunction {
        /**
         * Applies decorator to the given dispatcher.
         *
         * @param dispatcher event dispatcher to decorate
         * @return decorated event dispatcher
         */
        EventDispatcher apply(EventDispatcher dispatcher);
    }
}
