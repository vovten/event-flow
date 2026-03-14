package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import com.github.vovten.eventflow.transport.OutgoingEventTransportException;

import java.util.concurrent.BlockingDeque;

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
 * BlockingDeque<Event> queue = new LinkedBlockingDeque<>(1000);
 * OutgoingEventTransport transport = new InMemoryOutgoingEventTransport(queue);
 * EventChannel channel = new InternalEventChannel(List.of(transport));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class InMemoryOutgoingEventTransport implements OutgoingEventTransport {

    private final BlockingDeque<Event> eventQueue;

    /**
     * Create in-memory transport with existing queue.
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
}
