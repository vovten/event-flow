package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;

import java.util.List;

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
 *     List.of(new KafkaOutgoingEventTransport(bootstrapServers, "events"))
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
    List<OutgoingEventTransport> transports();

    /**
     * Send event to all transports associated with this channel.
     * <p>
     * This method iterates over all configured transports and sends
     * the event to each one. If a transport fails, the exception
     * propagates to the caller.
     *
     * @param event the event to send
     */
    default void send(Event event) {
        for (OutgoingEventTransport transport : transports()) {
            transport.send(event);
        }
    }
}
