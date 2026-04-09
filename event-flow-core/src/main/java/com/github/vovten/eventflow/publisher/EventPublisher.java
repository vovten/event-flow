package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.SendResult;

import java.util.List;
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
     * @return CompletableFuture that completes with list of SendResults from all channels
     */
    CompletableFuture<List<SendResult>> publish(Event event);
}
