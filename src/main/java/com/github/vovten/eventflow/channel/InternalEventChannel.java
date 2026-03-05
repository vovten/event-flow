package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.transport.EventTransport;

import java.util.List;

/**
 * Internal channel for in-application event delivery.
 * <p>
 * This channel is used to deliver events within a single application instance.
 * Events published to this channel are typically consumed by local event listeners
 * using an in-memory queue (e.g., {@code BlockingDeque}).
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
 *     List.of(new InMemoryEventTransport(1000))
 * );
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
public class InternalEventChannel implements EventChannel {
    
    private final List<EventTransport> transports;
    
    /**
     * Create internal channel with custom transports.
     *
     * @param transports list of transports for this channel
     */
    public InternalEventChannel(List<EventTransport> transports) {
        this.transports = transports;
    }

    /**
     * Create internal channel with a single transport.
     *
     * @param transport the transport for this channel
     */
    public InternalEventChannel(EventTransport transport) {
        this.transports = List.of(transport);
    }
    
    @Override
    public String name() {
        return "internal";
    }
    
    @Override
    public List<EventTransport> transports() {
        return transports;
    }
}
