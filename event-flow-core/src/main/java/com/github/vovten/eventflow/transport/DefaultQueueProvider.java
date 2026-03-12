package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.Event;

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Провайдер общей очереди для in-memory транспортов.
 * <p>
 * Использует одну очередь для всех in-memory транспортов в приложении.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class DefaultQueueProvider implements QueueProvider {

    private final int capacity;
    private final Map<String, BlockingDeque<Event>> queues;

    /**
     * Создает провайдер с очередью указанной емкости.
     *
     * @param capacity емкость очереди
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
