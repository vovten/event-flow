package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.Event;

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Default queue provider for in-memory transports.
 * <p>
 * Maintains separate queues for each transport name, creating them on demand.
 * All queues are bounded with the same capacity specified at provider creation.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class DefaultQueueProvider implements QueueProvider {

    private final int capacity;
    private final Map<String, BlockingDeque<Event>> queues;

    /**
     * Creates provider with queue of specified capacity.
     *
     * @param capacity queue capacity
     */
    public DefaultQueueProvider(int capacity) {
        this.capacity = capacity;
        this.queues = new ConcurrentHashMap<>();
    }

    @Override
    public BlockingDeque<Event> getQueue(String transportName) {
        return queues.computeIfAbsent(transportName, k -> new LinkedBlockingDeque<>(capacity));
    }

    @Override
    public boolean hasTransport(String transportName) {
        return queues.containsKey(transportName);
    }
}
