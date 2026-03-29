package com.github.vovten.eventflow.transport.incoming;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.InTransport;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Local-queue dispatcher transport for receiving internal events.
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
 * BlockingDeque<Event> queue = new LinkedBlockingDeque<>(1000);
 * DispatcherTransport transport = new LocalQueueDispatcherTransport(queue);
 * transport.start(event -> dispatcher.dispatch(event));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-06
 */
@Slf4j
public class LocalQueueInTransport implements InTransport {

    private final BlockingDeque<Event> eventQueue;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Create local-queue transport with existing queue and newSingleThreadExecutor service.
     *
     * @param eventQueue        the event queue to listen to
     */
    public LocalQueueInTransport(BlockingDeque<Event> eventQueue) {
        this.eventQueue = eventQueue;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * Create local-queue transport with existing queue and executor service.
     *
     * @param eventQueue        the event queue to listen to
     * @param executorService   executor service for running the consumer loop
     */
    public LocalQueueInTransport(BlockingDeque<Event> eventQueue, ExecutorService executorService) {
        this.eventQueue = eventQueue;
        this.executorService = executorService;
    }

    @Override
    public String name() {
        return "local-queue";
    }

    @Override
    public void start(Consumer<Event> eventConsumer) {
        if (running.compareAndSet(false, true)) {
            executorService.execute(() -> consumeLoop(eventConsumer));
            log.debug("LocalQueueDispatcherTransport started");
        } else {
            log.warn("LocalQueueDispatcherTransport is already running");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("LocalQueueDispatcherTransport stopped");
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdownNow();
            }
        }
    }

    private void consumeLoop(Consumer<Event> eventConsumer) {
        try {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    Event event = eventQueue.take();
                    tryDeliver(event, eventConsumer);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.debug("LocalQueueDispatcherTransport consumer loop interrupted");
                    break;
                } catch (Exception e) {
                    if (running.get()) {
                        log.error("Error consuming event from queue", e);
                    }
                }
            }
        } catch (Exception e) {
            if (running.get()) {
                log.error("LocalQueueDispatcherTransport loop error", e);
            }
        }
    }

    private void tryDeliver(Event event, Consumer<Event> eventConsumer) {
        try {
            eventConsumer.accept(event);
            log.debug("Event delivered from local-queue: {}", event.type().getSimpleName());
        } catch (Exception e) {
            log.error("Error delivering event from local-queue: {}", event, e);
        }
    }
}
