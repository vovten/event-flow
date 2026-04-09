package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Event channel — a logical route for event delivery.
 * <p>
 * A channel defines which transports will be used to deliver an event.
 * Channels decouple event publishers from specific transport mechanisms,
 * allowing flexible configuration of event routing.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Create channel with multiple transports
 * EventChannel channel = new ExternalEventChannel(
 *     List.of(new KafkaPublisherTransport(bootstrapServers, "events"))
 * );
 *
 * // Send event through channel
 * channel.send(event);
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public interface EventChannel {

    /**
     * @return unique channel name (e.g., "internal", "external")
     */
    String name();

    /**
     * @return list of transports configured for this channel
     */
    List<OutTransport> transports();

    /**
     * Send event to all transports associated with this channel asynchronously.
     * <p>
     * This method iterates over all configured transports and sends
     * the event to each one. Returns a CompletableFuture that completes
     * when all transports have finished sending.
     *
     * @param event the event to send
     * @return CompletableFuture that completes with list of SendResults from all transports
     */
    CompletableFuture<List<SendResult>> send(Event event);
}
