package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.transport.OutTransport;

import java.util.List;

/**
 * Internal channel for in-application event delivery.
 * <p>
 * This channel is used to deliver events within a single application instance.
 * Events published to this channel are typically consumed by local event listeners
 * using a local-queue (e.g., {@code BlockingDeque}).
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Communication between components within the same application</li>
 *   <li>Asynchronous processing of local events</li>
 *   <li>Decoupling event producers from consumers inside the application</li>
 * </ul>
 * <p>
 * <b>Configuration example:</b>
 * <pre>{@code
 * EventChannel internalChannel = new InternalEventChannel(
 *     List.of(new LocalQueuePublisherTransport(queue))
 * );
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class InternalEventChannel extends AbstractEventChannel {

    /**
     * Create internal channel with custom transports.
     *
     * @param transports list of transports for this channel
     */
    public InternalEventChannel(List<OutTransport> transports) {
        super(transports);
    }

    /**
     * Create internal channel with a single transport.
     *
     * @param transport the transport for this channel
     */
    public InternalEventChannel(OutTransport transport) {
        super(transport);
    }

    @Override
    public String name() {
        return "internal";
    }
}
