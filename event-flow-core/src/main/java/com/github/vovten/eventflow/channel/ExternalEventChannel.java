package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.transport.OutTransport;

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
 *     List.of(new KafkaPublisherTransport(bootstrapServers, "events"))
 * );
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class ExternalEventChannel extends AbstractEventChannel {

    /**
     * Create external channel with custom transports.
     *
     * @param transports list of transports for this channel
     */
    public ExternalEventChannel(List<OutTransport> transports) {
        super(transports);
    }

    /**
     * Create external channel with a single transport.
     *
     * @param transport the transport for this channel
     */
    public ExternalEventChannel(OutTransport transport) {
        super(transport);
    }

    @Override
    public String name() {
        return "external";
    }
}
