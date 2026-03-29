package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.event.Event;

import java.util.function.Consumer;

/**
 * Incoming transport — a mechanism for receiving events from external sources.
 * <p>
 * An incoming transport is responsible for receiving events from a source
 * (e.g., in-memory queue, Kafka topic) and delivering them
 * to the event dispatcher for further processing by listeners.
 * <p>
 * <b>Implementations:</b>
 * <ul>
 *   <li>{@link com.github.vovten.eventflow.transport.incoming.LocalQueueInTransport} — receives events from in-memory queue</li>
 *   <li>{@link com.github.vovten.eventflow.transport.incoming.KafkaInTransport} — receives events from Kafka topic</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * InTransport transport = new KafkaInTransport(bootstrapServers, "events", "group-1");
 * transport.start(event -> dispatcher.dispatch(event));
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-06
 */
public interface InTransport {

    /**
     * @return unique transport name (e.g., "kafka", "in-memory")
     */
    String name();

    /**
     * Start receiving events and deliver them to the specified consumer.
     * <p>
     * This method should be called to activate the transport. It will start
     * listening for events from the source and deliver them to the provided
     * consumer as they arrive.
     *
     * @param eventConsumer the consumer to deliver events to
     */
    void start(Consumer<Event> eventConsumer);

    /**
     * Stop receiving events.
     * <p>
     * This method should be called to gracefully shut down the transport.
     * Any resources (e.g., connections, threads) should be released.
     */
    void stop();
}
