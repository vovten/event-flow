package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.Event;

/**
 * Outgoing event transport — a mechanism for sending events to external destinations.
 * <p>
 * A transport is responsible for the actual delivery of events to their destination.
 * Transports are configured within channels and are transparent to event publishers.
 * <p>
 * <b>Implementations:</b>
 * <ul>
 *   <li>InMemoryOutgoingEventTransport — in-memory queue for local delivery</li>
 *   <li>KafkaOutgoingEventTransport — Apache Kafka for distributed delivery</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * OutgoingEventTransport transport = new KafkaOutgoingEventTransport(bootstrapServers, "events");
 * transport.send(event);
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public interface OutgoingEventTransport {

    /**
     * @return unique transport name (e.g., "kafka", "in-memory")
     */
    String name();

    /**
     * Send an event.
     * <p>
     * Implementations should handle the actual delivery of the event.
     * Any exceptions during delivery will propagate to the caller.
     *
     * @param event the event to send
     */
    void send(Event event);
}
