package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import com.github.vovten.eventflow.transport.OutgoingEventTransportException;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * In-memory outgoing transport for internal event delivery.
 * <p>
 * This transport uses a bounded {@link BlockingDeque} to queue events
 * for consumption by local event dispatchers. It provides backpressure support
 * by rejecting events when the queue is full.
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
 * OutgoingEventTransport transport = new InMemoryOutgoingEventTransport(1000);
 * EventChannel channel = new InternalEventChannel(List.of(transport));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class InMemoryOutgoingEventTransport implements OutgoingEventTransport {

    private final BlockingDeque<Event> eventQueue;

    /**
     * Default queue size when not specified.
     */
    private static final int DEFAULT_QUEUE_SIZE = 5000;

    /**
     * Create in-memory transport with default queue size (5000).
     */
    public InMemoryOutgoingEventTransport() {
        this.eventQueue = new LinkedBlockingDeque<>(DEFAULT_QUEUE_SIZE);
    }

    /**
     * Create in-memory transport with custom queue size.
     *
     * @param maxQueueSize maximum queue size for backpressure
     */
    public InMemoryOutgoingEventTransport(int maxQueueSize) {
        this.eventQueue = new LinkedBlockingDeque<>(maxQueueSize);
    }

    /**
     * Create in-memory transport with existing queue.
     * <p>
     * This constructor allows sharing the same queue with other transports,
     * such as {@link InMemoryIncomingEventTransport}.
     *
     * @param eventQueue the event queue to use
     */
    public InMemoryOutgoingEventTransport(BlockingDeque<Event> eventQueue) {
        this.eventQueue = eventQueue;
    }

    @Override
    public String name() {
        return "in-memory";
    }

    @Override
    public void send(Event event) {
        if (!eventQueue.offer(event)) {
            throw new OutgoingEventTransportException("Queue is full, event rejected: " + event);
        }
    }

    /**
     * @return the event queue for consumption by dispatchers
     */
    public BlockingDeque<Event> getEventQueue() {
        return eventQueue;
    }
}
