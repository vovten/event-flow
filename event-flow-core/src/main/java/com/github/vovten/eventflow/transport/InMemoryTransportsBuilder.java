package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.dispatcher.InMemoryDispatcherTransport;
import com.github.vovten.eventflow.transport.publisher.InMemoryPublisherTransport;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Builder for creating a pair of in-memory dispatcher and publisher event transports.
 * <p>
 * This builder creates both transports sharing the same {@link BlockingDeque} for
 * in-process event communication. The publisher transport puts events into the queue,
 * and the dispatcher transport consumes events from the same queue.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * InMemoryTransportsBuilder builder = new InMemoryTransportsBuilder()
 *     .queueSize(1000);
 *
 * InMemoryTransportsBuilder.InMemoryTransports transports = builder.build();
 * PublisherTransport publisher = transports.publisher();
 * DispatcherTransport dispatcher = transports.dispatcher();
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
public class InMemoryTransportsBuilder {

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
    public InMemoryTransportsBuilder() {
        this.queueSize = DEFAULT_QUEUE_SIZE;
    }

    /**
     * Set the queue size for the shared event queue.
     *
     * @param queueSize the maximum size of the queue
     * @return this builder
     */
    public InMemoryTransportsBuilder queueSize(int queueSize) {
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
    public InMemoryTransportsBuilder queue(BlockingDeque<Event> eventQueue) {
        this.eventQueue = eventQueue;
        return this;
    }

    /**
     * Set a custom executor service for the dispatcher transport.
     *
     * @param executorService the executor service
     * @return this builder
     */
    public InMemoryTransportsBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Build and return a pair of in-memory transports sharing the same queue.
     *
     * @return a pair of dispatcher and publisher transports
     */
    public InMemoryTransports build() {
        if (eventQueue == null) {
            eventQueue = new LinkedBlockingDeque<>(queueSize);
        }

        ExecutorService executor = executorService != null
                ? executorService
                : Executors.newSingleThreadExecutor();

        InMemoryDispatcherTransport dispatcher = new InMemoryDispatcherTransport(eventQueue, executor);
        InMemoryPublisherTransport publisher = new InMemoryPublisherTransport(eventQueue);

        return new InMemoryTransports(dispatcher, publisher);
    }

    /**
     * Record holding a pair of in-memory transports.
     *
     * @param dispatcher  the dispatcher transport
     * @param publisher  the publisher transport
     */
    public record InMemoryTransports(
            InMemoryDispatcherTransport dispatcher,
            InMemoryPublisherTransport publisher
    ) {
    }
}
