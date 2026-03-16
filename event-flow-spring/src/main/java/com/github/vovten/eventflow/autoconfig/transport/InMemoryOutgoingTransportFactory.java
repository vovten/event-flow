package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import com.github.vovten.eventflow.transport.QueueProvider;
import com.github.vovten.eventflow.transport.outgoing.InMemoryOutgoingEventTransport;

/**
 * Factory for creating in-memory outgoing event transports.
 * <p>
 * Uses a shared {@link DefaultQueueProvider} for all in-memory transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class InMemoryOutgoingTransportFactory implements OutgoingTransportFactory {

    private final QueueProvider queueProvider;

    public InMemoryOutgoingTransportFactory(DefaultQueueProvider queueProvider) {
        this.queueProvider = queueProvider;
    }

    @Override
    public String getName() {
        return "in-memory";
    }

    @Override
    public OutgoingEventTransport createOutgoing(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new InMemoryOutgoingEventTransport(queueProvider.getQueue(config.getName()));
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        // In-memory transport requires no additional configuration
    }
}
