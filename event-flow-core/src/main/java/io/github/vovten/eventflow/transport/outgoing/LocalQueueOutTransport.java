package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.SendResult;

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;

/**
 * In-JVM transport that queues events using a bounded {@link BlockingDeque}.
 * Provides backpressure by returning a failed SendResult when the queue is full.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class LocalQueueOutTransport implements OutTransport {

    private final BlockingDeque<Event> eventQueue;

    /**
     * Create local-queue transport with an existing queue.
     *
     * @param eventQueue the event queue to use
     */
    public LocalQueueOutTransport(BlockingDeque<Event> eventQueue) {
        this.eventQueue = eventQueue;
    }

    @Override
    public String name() {
        return "local-queue";
    }

    @Override
    public CompletableFuture<SendResult> send(Event event) {
        if (!eventQueue.offer(event)) {
            return CompletableFuture.completedFuture(
                    SendResult.failure(name(), null, "Queue is full, event rejected")
            );
        }
        Map<String, Object> metadata = Map.of("queueSize", eventQueue.size());
        return CompletableFuture.completedFuture(SendResult.success(name(), metadata));
    }
}
