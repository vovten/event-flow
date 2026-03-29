package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.event.Event;

/**
 * Outgoing transport — a mechanism for sending events to external destinations.
 * <p>
 * An outgoing transport is responsible for the actual delivery of events to their destination.
 * Transports are configured within channels and are transparent to event publishers.
 * <p>
 * <b>Implementations:</b>
 * <ul>
 *   <li>{@link com.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport} — local-queue for local delivery</li>
 *   <li>{@link com.github.vovten.eventflow.transport.outgoing.KafkaOutTransport} — Apache Kafka for distributed delivery</li>
 *   <li>{@link com.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutTransport} — sends to all Kafka topic partitions</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * OutTransport transport = new KafkaOutTransport(bootstrapServers, "events");
 * transport.send(event);
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public interface OutTransport {

    /**
     * @return unique transport name (e.g., "kafka", "local-queue")
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
