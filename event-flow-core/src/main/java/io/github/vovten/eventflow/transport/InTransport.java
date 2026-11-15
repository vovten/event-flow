package io.github.vovten.eventflow.transport;

/**
 * Incoming transport — a mechanism for receiving events from external sources.
 * <p>
 * An incoming transport is responsible for receiving events from a source
 * (e.g., local-queue, Kafka topic) and delivering them
 * to the event dispatcher for further processing by listeners.
 * <p>
 * <b>Implementations:</b>
 * <ul>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.LocalQueueInTransport} — receives events from local-queue</li>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.KafkaInTransport} — receives events from Kafka topic</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * InTransport transport = new KafkaInTransport(bootstrapServers, "events", "group-1");
 * transport.start(dispatcher::dispatch);
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public interface InTransport {

    /**
     * @return unique transport name (e.g., "kafka", "local-queue")
     */
    String name();

    /**
     * Start receiving events and deliver them to the specified consumer.
     * <p>
     * This method should be called to activate the transport. It will start
     * listening for events from the source and deliver them to the provided
     * consumer as they arrive. The transport may rely on the returned future
     * to acknowledge the source only after processing has completed
     * (at-least-once delivery).
     *
     * @param eventConsumer the consumer to deliver events to
     */
    void start(AsyncEventConsumer eventConsumer);

    /**
     * Stop receiving events.
     * <p>
     * This method should be called to gracefully shut down the transport.
     * Any resources (e.g., connections, threads) should be released.
     */
    void stop();
}
