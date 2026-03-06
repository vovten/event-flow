package com.github.vovten.eventflow.transport.incoming;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

/**
 * In-memory incoming transport for receiving internal events.
 * <p>
 * This transport listens to a bounded {@link BlockingDeque} and delivers events
 * to the registered consumer. It provides backpressure support by rejecting events
 * when the queue is full.
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>In-application event communication</li>
 *   <li>Asynchronous processing within a single JVM</li>
 *   <li>Lightweight event queuing without external dependencies</li>
 * </ul>
 * <p>
 * <b>Configuration example:</b>
 * <pre>{@code
 * IncomingEventTransport transport = new InMemoryIncomingEventTransport(1000);
 * transport.start(event -> dispatcher.dispatch(event));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-06
 */
@Slf4j
public class InMemoryIncomingEventTransport implements IncomingEventTransport {

    private final BlockingDeque<Event> eventQueue;
    private final ExecutorService executorService;
    private volatile boolean running = false;

    /**
     * Default queue size when not specified.
     */
    private static final int DEFAULT_QUEUE_SIZE = 1000;

    /**
     * Create in-memory transport with default queue size (1000).
     */
    public InMemoryIncomingEventTransport() {
        this(new LinkedBlockingDeque<>(DEFAULT_QUEUE_SIZE), Executors.newSingleThreadExecutor());
    }

    /**
     * Create in-memory transport with custom queue size.
     *
     * @param maxQueueSize maximum queue size for backpressure
     */
    public InMemoryIncomingEventTransport(int maxQueueSize) {
        this(new LinkedBlockingDeque<>(maxQueueSize), Executors.newSingleThreadExecutor());
    }

    /**
     * Create in-memory transport with existing queue.
     *
     * @param eventQueue the event queue to listen to
     */
    public InMemoryIncomingEventTransport(BlockingDeque<Event> eventQueue) {
        this(eventQueue, Executors.newSingleThreadExecutor());
    }

    /**
     * Create in-memory transport with existing queue and executor service.
     *
     * @param eventQueue        the event queue to listen to
     * @param executorService   executor service for running the consumer loop
     */
    public InMemoryIncomingEventTransport(BlockingDeque<Event> eventQueue, ExecutorService executorService) {
        this.eventQueue = eventQueue;
        this.executorService = executorService;
    }

    @Override
    public String name() {
        return "in-memory";
    }

    @Override
    public void start(Consumer<Event> eventConsumer) {
        if (running) {
            log.warn("InMemoryIncomingEventTransport is already running");
            return;
        }
        running = true;
        executorService.execute(() -> consumeLoop(eventConsumer));
        log.debug("InMemoryIncomingEventTransport started");
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
        log.info("InMemoryIncomingEventTransport stopped");
    }

    private void consumeLoop(Consumer<Event> eventConsumer) {
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    Event event = eventQueue.take();
                    tryDeliver(event, eventConsumer);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.debug("InMemoryIncomingEventTransport consumer loop interrupted");
                    break;
                } catch (Exception e) {
                    if (running) {
                        log.error("Error consuming event from queue", e);
                    }
                }
            }
        } catch (Exception e) {
            if (running) {
                log.error("InMemoryIncomingEventTransport loop error", e);
            }
        }
    }

    private void tryDeliver(Event event, Consumer<Event> eventConsumer) {
        try {
            eventConsumer.accept(event);
            log.debug("Event delivered from in-memory queue: {}", event.type().getSimpleName());
        } catch (Exception e) {
            log.error("Error delivering event from in-memory queue: {}", event, e);
        }
    }
}
