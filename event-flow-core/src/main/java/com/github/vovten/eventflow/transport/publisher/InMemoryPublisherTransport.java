package com.github.vovten.eventflow.transport.publisher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.PublisherTransport;
import com.github.vovten.eventflow.transport.TransportException;

import java.util.concurrent.BlockingDeque;

/**
 * In-memory publisher transport for internal event delivery.
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
 * PublisherTransport transport = new InMemoryPublisherTransport(queue);
 * EventChannel channel = new InternalEventChannel(List.of(transport));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class InMemoryPublisherTransport implements PublisherTransport {

    private final BlockingDeque<Event> eventQueue;

    /**
     * Create in-memory transport with existing queue.
     *
     * @param eventQueue the event queue to use
     */
    public InMemoryPublisherTransport(BlockingDeque<Event> eventQueue) {
        this.eventQueue = eventQueue;
    }

    @Override
    public String name() {
        return "in-memory";
    }

    @Override
    public void send(Event event) {
        if (!eventQueue.offer(event)) {
            throw new TransportException("Queue is full, event rejected: " + event);
        }
    }
}
