package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.EventHandler;
import com.github.vovten.eventflow.registry.EventHandlerRegistry;
import com.github.vovten.eventflow.transport.InTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
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
 * @since 2026-03-06
 */
public class UnifiedEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(UnifiedEventDispatcher.class);

    private final ExecutorService executorService;
    private final EventHandlerRegistry handlerRegistry;
    private final List<InTransport> transports;
    private final Semaphore concurrencySemaphore;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Create unified dispatcher with custom handler registry.
     * <p>
     * <b>Resource ownership:</b> This dispatcher does NOT close the provided
     * {@code executorService}. The caller is responsible for shutting down the executor
     * when it is no longer needed. This follows the standard dependency injection principle
     * where the component that creates a resource is responsible for its lifecycle.
     * <p>
     * <b>Usage example:</b>
     * <pre>{@code
     * ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
     * EventDispatcher dispatcher = new UnifiedEventDispatcher(executor, registry, transports);
     *
     * try {
     *     dispatcher.start(consumer);
     *     // ... application work ...
     * } finally {
     *     dispatcher.stop();
     *     executor.shutdownNow(); // Caller must close executor
     * }
     * }</pre>
     *
     * @param executorService  executor service for async handler execution (NOT closed by dispatcher)
     * @param handlerRegistry  custom handler registry
     * @param transports       list of transports to listen to
     */
    public UnifiedEventDispatcher(ExecutorService executorService,
                                  EventHandlerRegistry handlerRegistry,
                                  List<InTransport> transports) {
        this(executorService, handlerRegistry, transports, null);
    }

    /**
     * Create unified dispatcher with concurrency limiting for backpressure support.
     * <p>
     * When using virtual threads ({@code Executors.newVirtualThreadPerTaskExecutor()}),
     * the executor never rejects tasks. Without a concurrency limit, a burst of events
     * can spawn thousands of concurrent handler executions, potentially overwhelming
     * downstream systems (databases, HTTP services).
     * <p>
     * The {@code concurrencySemaphore} limits the number of handler executions running
     * simultaneously. When the semaphore is exhausted, handler submissions block until
     * a slot becomes available, providing backpressure without rejecting events.
     * <p>
     * <b>Resource ownership:</b> This dispatcher does NOT close the provided
     * {@code executorService}. The caller is responsible for shutting down the executor.
     *
     * @param executorService       executor service for async handler execution (NOT closed by dispatcher)
     * @param handlerRegistry       custom handler registry
     * @param transports            list of transports to listen to
     * @param concurrencySemaphore  semaphore for limiting concurrent handler executions (null = unlimited)
     */
    public UnifiedEventDispatcher(ExecutorService executorService,
                                  EventHandlerRegistry handlerRegistry,
                                  List<InTransport> transports,
                                  Semaphore concurrencySemaphore) {
        this.transports = transports;
        this.executorService = executorService;
        this.handlerRegistry = handlerRegistry;
        this.concurrencySemaphore = concurrencySemaphore;
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
    public void dispatch(Event event) {
        List<EventHandler> handlers = handlerRegistry.getHandlers(event);
        if (handlers.isEmpty()) {
            log.debug("No handlers found for event: {}", event);
            return;
        }
        int totalHandlers = handlers.size();
        int submittedHandlers = 0;

        for (EventHandler handler : handlers) {
            try {
                if (concurrencySemaphore != null) {
                    concurrencySemaphore.acquire();
                }
                executorService.execute(() -> {
                    try {
                        handler.onEvent(event);
                    } finally {
                        if (concurrencySemaphore != null) {
                            concurrencySemaphore.release();
                        }
                    }
                });
                submittedHandlers++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Handler submission interrupted for event {} (handler {}/{}): {}",
                        event.type().getSimpleName(),
                        submittedHandlers + 1,
                        totalHandlers,
                        handler.getClass().getSimpleName(),
                        e);
                break;
            } catch (Exception e) {
                log.error("Failed to submit handler for event {} (handler {}/{}): {}",
                        event.type().getSimpleName(),
                        submittedHandlers + 1,
                        totalHandlers,
                        handler.getClass().getSimpleName(),
                        e);
            }
        }
        if (submittedHandlers < totalHandlers) {
            log.warn("Partial handler submission for event {}: {}/{} handlers submitted",
                    event.type().getSimpleName(), submittedHandlers, totalHandlers);
        }
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
}
