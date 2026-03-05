package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.Event;

/**
 * Event transport — a mechanism for physical event delivery.
 * <p>
 * A transport is responsible for the actual delivery of events to their destination.
 * Transports are configured within channels and are transparent to event publishers.
 * <p>
 * <b>Implementations:</b>
 * <ul>
 *   <li>InMemoryEventTransport — in-memory queue for local delivery</li>
 *   <li>KafkaEventTransport — Apache Kafka for distributed delivery</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * EventTransport transport = new KafkaEventTransport(bootstrapServers, "events");
 * transport.send(event);
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public interface EventTransport {
    
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
