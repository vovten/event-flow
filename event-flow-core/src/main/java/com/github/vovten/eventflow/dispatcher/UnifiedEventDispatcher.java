package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.EventHandler;
import com.github.vovten.eventflow.registry.EventHandlerRegistry;
import com.github.vovten.eventflow.transport.InTransport;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
 *   <li>Support for multiple transports (in-memory, Kafka, etc.)</li>
 *   <li>Thread-safe event delivery to handlers</li>
 *   <li>External handler registry injection</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>{@code
 * DispatcherTransport(s) → UnifiedEventDispatcher → EventHandler(s)
 *      ↓ Kafka
 *      ↓ In-Memory
 *      ↓ Custom...
 * }</pre>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Create transports
 * DispatcherTransport memoryTransport = new InMemoryDispatcherTransport(queue);
 * DispatcherTransport kafkaTransport = new KafkaDispatcherTransport(
 *     "localhost:9092", "events", "my-group"
 * );
 *
 * // Create handler registry
 * EventHandlerRegistry registry = new CompositeEventHandlerRegistry(
 *     List.of(annotationRegistry, subscriberRegistry)
 * );
 *
 * // Create dispatcher with multiple transports
 * UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
 *     executorService,
 *     List.of(memoryTransport, kafkaTransport),
 *     registry
 * );
 * dispatcher.start();
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-06
 */
@Slf4j
public class UnifiedEventDispatcher implements EventDispatcher {

    private final ExecutorService executorService;
    private final EventHandlerRegistry handlerRegistry;
    private final List<InTransport> transports;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Create unified dispatcher with custom handler registry.
     *
     * @param executorService  executor service for async handler execution
     * @param handlerRegistry  custom handler registry
     * @param transports       list of transports to listen to
     */
    public UnifiedEventDispatcher(ExecutorService executorService,
                                  EventHandlerRegistry handlerRegistry,
                                  List<InTransport> transports) {
        this.transports = transports;
        this.executorService = executorService;
        this.handlerRegistry = handlerRegistry;
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            for (InTransport transport : transports) {
                transport.start(this::dispatch);
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
        for (EventHandler handler : handlers) {
            executorService.execute(() -> handler.onEvent(event));
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
        String msg = "UnifiedEventDispatcher started with %s transport(s): %s";
        String names = transports.stream().map(InTransport::name).collect(joining(","));
        return String.format(msg, transports.size(), names);
    }
}
