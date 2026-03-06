package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unified event dispatcher that listens to events from multiple transport sources.
 * <p>
 * This dispatcher can listen to multiple
 * {@link IncomingEventTransport} instances simultaneously, delivering events from all
 * sources to the registered listeners.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Single dispatcher implementation for all transport types</li>
 *   <li>Support for multiple transports (in-memory, Kafka, etc.)</li>
 *   <li>Thread-safe event delivery to listeners</li>
 *   <li>External listener registry injection</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>{@code
 * IncomingEventTransport(s) → UnifiedEventDispatcher → EventListener(s)
 *      ↓ Kafka
 *      ↓ In-Memory
 *      ↓ Custom...
 * }</pre>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Create transports
 * IncomingEventTransport memoryTransport = new InMemoryIncomingEventTransport(1000);
 * IncomingEventTransport kafkaTransport = new KafkaIncomingEventTransport(
 *     "localhost:9092", "events", "my-group"
 * );
 *
 * // Create listener registry
 * EventListenerRegistry registry = new CompositeEventListenerRegistry(
 *     List.of(annotationRegistry, interfaceRegistry)
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
    private final EventListenerRegistry listenerRegistry;
    private final List<IncomingEventTransport> transports;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Create unified dispatcher with custom listener registry.
     *
     * @param executorService  executor service for async listener execution
     * @param listenerRegistry custom listener registry
     * @param transports       list of transports to listen to
     */
    public UnifiedEventDispatcher(ExecutorService executorService,
                                  EventListenerRegistry listenerRegistry,
                                  List<IncomingEventTransport> transports) {
        this.transports = transports;
        this.executorService = executorService;
        this.listenerRegistry = listenerRegistry;
    }

    /**
     * Start the dispatcher and all configured transports.
     * <p>
     * This method activates all transports and begins delivering events to
     * registered listeners.
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            for (IncomingEventTransport transport : transports) {
                transport.start(this::dispatch);
            }
            log.info("UnifiedEventDispatcher started with {} transport(s)", transports.size());
        } else {
            log.warn("UnifiedEventDispatcher is already started");
        }
    }

    /**
     * Stop the dispatcher and all configured transports.
     * <p>
     * This method gracefully shuts down all transports and releases resources.
     */
    public void stop() {
        if (started.compareAndSet(true, false)) {
            log.info("Stopping UnifiedEventDispatcher...");
            for (IncomingEventTransport transport : transports) {
                tryStop(transport);
            }
            log.info("UnifiedEventDispatcher stopped");
        }
    }

    @Override
    public void dispatch(Event event) {
        List<EventListener> listeners = listenerRegistry.getListeners(event);
        if (listeners.isEmpty()) {
            log.debug("No listeners found for event: {}", event);
            return;
        }
        for (EventListener listener : listeners) {
            executorService.execute(() -> listener.onEvent(event));
        }
    }

    @Override
    public void register(Object listener) {
        if (!isRegistered(listener)) {
            listenerRegistry.register(listener);
        }
    }

    @Override
    public boolean isRegistered(Object listener) {
        return listenerRegistry.isRegistered(listener);
    }

    private void tryStop(IncomingEventTransport transport) {
        try {
            transport.stop();
        } catch (Exception e) {
            log.warn("Error stopping transport {}", transport.name(), e);
        }
    }
}
