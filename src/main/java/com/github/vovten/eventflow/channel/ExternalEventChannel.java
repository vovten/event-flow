package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.transport.OutgoingEventTransport;

import java.util.List;

/**
 * External channel for cross-application event delivery.
 * <p>
 * This channel is used to deliver events to external systems, such as other
 * microservices or applications. Events published to this channel are typically
 * sent to a message broker (e.g., Kafka, RabbitMQ) for distribution.
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Communication between different applications or microservices</li>
 *   <li>Event-driven architecture with external consumers</li>
 *   <li>Audit logging and event sourcing</li>
 *   <li>Integration with third-party systems</li>
 * </ul>
 * <p>
 * <b>Configuration example:</b>
 * <pre>{@code
 * EventChannel externalChannel = new ExternalEventChannel(
 *     List.of(new KafkaOutgoingEventTransport(bootstrapServers, "events"))
 * );
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class ExternalEventChannel implements EventChannel {

    private final List<OutgoingEventTransport> transports;

    /**
     * Create external channel with custom transports.
     *
     * @param transports list of transports for this channel
     */
    public ExternalEventChannel(List<OutgoingEventTransport> transports) {
        this.transports = transports;
    }

    /**
     * Create external channel with a single transport.
     *
     * @param transport the transport for this channel
     */
    public ExternalEventChannel(OutgoingEventTransport transport) {
        this.transports = List.of(transport);
    }

    @Override
    public String name() {
        return "external";
    }

    @Override
    public List<OutgoingEventTransport> transports() {
        return transports;
    }
}
