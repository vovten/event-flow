package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.event.Event;

import java.util.concurrent.BlockingDeque;

/**
 * Queue provider for in-memory transports.
 * <p>
 * Allows getting a shared queue by transport name,
 * which provides communication between publisher and dispatcher.
 */
public interface LocalQueueProvider {

    /**
     * Gets queue for transport with specified name.
     *
     * @param transportName transport name
     * @return event queue
     * @throws IllegalArgumentException if transport with given name is not found
     */
    BlockingDeque<Event> getQueue(String transportName);

    /**
     * Checks if transport with specified name exists.
     *
     * @param transportName transport name
     * @return true if transport exists
     */
    boolean hasTransport(String transportName);
}
