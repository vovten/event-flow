package io.github.vovten.eventflow.transport;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.incoming.LocalQueueInTransport;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Builder for creating a pair of local-queue dispatcher and publisher event transports.
 * <p>
 * This builder creates both transports sharing the same {@link BlockingDeque} for
 * in-process event communication. The publisher transport puts events into the queue,
 * and the dispatcher transport consumes events from the same queue.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * LocalQueueTransportsBuilder builder = new LocalQueueTransportsBuilder()
 *     .queueSize(1000);
 *
 * LocalQueueTransportsBuilder.LocalQueueTransports transports = builder.build();
 * PublisherTransport publisher = transports.publisher();
 * DispatcherTransport dispatcher = transports.dispatcher();
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
public class LocalQueueTransportsBuilder {

    private BlockingDeque<Event> eventQueue;
    private int queueSize;
    private ExecutorService executorService;

    /**
     * Default queue size when not specified.
     */
    private static final int DEFAULT_QUEUE_SIZE = 1000;

    /**
     * Create a new builder with default queue size.
     */
    public LocalQueueTransportsBuilder() {
        this.queueSize = DEFAULT_QUEUE_SIZE;
    }

    /**
     * Set the queue size for the shared event queue.
     *
     * @param queueSize the maximum size of the queue
     * @return this builder
     */
    public LocalQueueTransportsBuilder queueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    /**
     * Set a custom event queue to be shared between transports.
     * <p>
     * When using this method, the {@link #queueSize(int)} setting is ignored.
     *
     * @param eventQueue the shared event queue
     * @return this builder
     */
    public LocalQueueTransportsBuilder queue(BlockingDeque<Event> eventQueue) {
        this.eventQueue = eventQueue;
        return this;
    }

    /**
     * Set a custom executor service for the dispatcher transport.
     *
     * @param executorService the executor service
     * @return this builder
     */
    public LocalQueueTransportsBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Build and return a pair of local-queue transports sharing the same queue.
     *
     * @return a pair of dispatcher and publisher transports
     */
    public LocalQueueTransports build() {
        if (eventQueue == null) {
            eventQueue = new LinkedBlockingDeque<>(queueSize);
        }

        ExecutorService executor = executorService != null
                ? executorService
                : Executors.newSingleThreadExecutor();

        LocalQueueInTransport dispatcher = new LocalQueueInTransport(eventQueue, executor);
        LocalQueueOutTransport publisher = new LocalQueueOutTransport(eventQueue);

        return new LocalQueueTransports(dispatcher, publisher);
    }

    /**
     * Record holding a pair of local-queue transports.
     *
     * @param dispatcher  the dispatcher transport
     * @param publisher  the publisher transport
     */
    public record LocalQueueTransports(
            LocalQueueInTransport dispatcher,
            LocalQueueOutTransport publisher
    ) {
    }
}
