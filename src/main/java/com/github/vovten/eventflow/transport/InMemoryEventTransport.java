package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.Event;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * In-memory transport for internal event delivery.
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
 * EventTransport transport = new InMemoryEventTransport(1000);
 * EventChannel channel = new InternalEventChannel(List.of(transport));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class InMemoryEventTransport implements EventTransport {
    
    private final BlockingDeque<Event> eventQueue;
    
    /**
     * Create in-memory transport with custom queue size.
     *
     * @param maxQueueSize maximum queue size for backpressure
     */
    public InMemoryEventTransport(int maxQueueSize) {
        this.eventQueue = new LinkedBlockingDeque<>(maxQueueSize);
    }
    
    @Override
    public String name() {
        return "in-memory";
    }
    
    @Override
    public void send(Event event) {
        if (!eventQueue.offer(event)) {
            throw new EventTransportException("Queue is full, event rejected: " + event);
        }
    }
    
    /**
     * @return the event queue for consumption by dispatchers
     */
    public BlockingDeque<Event> getEventQueue() {
        return eventQueue;
    }
}
