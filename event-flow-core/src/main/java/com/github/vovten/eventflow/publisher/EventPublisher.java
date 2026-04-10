package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.SendResults;

import java.util.concurrent.CompletableFuture;

/**
 * Event publisher interface.
 *
 * @author Vladimir Aleshkov
 * @since 2024-11-20
 */
public interface EventPublisher {

    /**
     * Publish the event asynchronously.
     *
     * @param event the event to publish
     * @return CompletableFuture that completes with SendResults
     */
    CompletableFuture<SendResults> publish(Event event);
}
