package io.github.vovten.eventflow.transport;

import io.github.vovten.eventflow.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Outgoing transport — a mechanism for sending events to external destinations.
 * <p>
 * An outgoing transport is responsible for the actual delivery of events to their destination.
 * Transports are configured within channels and are transparent to event publishers.
 * <p>
 * <b>Implementations:</b>
 * <ul>
 *   <li>{@link io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport} — local-queue for local delivery</li>
 *   <li>{@link io.github.vovten.eventflow.transport.outgoing.KafkaOutTransport} — Apache Kafka for distributed delivery</li>
 *   <li>{@link io.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutTransport} — sends to all Kafka topic partitions</li>
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
     * Send an event asynchronously.
     *
     * @param event the event to send
     * @return CompletableFuture that completes with SendResult
     */
    CompletableFuture<SendResult> send(Event event);
}
