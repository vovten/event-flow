package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import com.github.vovten.eventflow.transport.incoming.InMemoryIncomingEventTransport;

/**
 * Factory for creating in-memory incoming event transports.
 * <p>
 * Uses a shared {@link DefaultQueueProvider} for all in-memory transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class InMemoryIncomingTransportFactory implements IncomingTransportFactory {

    private final DefaultQueueProvider queueProvider;

    public InMemoryIncomingTransportFactory(DefaultQueueProvider queueProvider) {
        this.queueProvider = queueProvider;
    }

    @Override
    public String getType() {
        return "in-memory";
    }

    @Override
    public IncomingEventTransport createIncoming(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new InMemoryIncomingEventTransport(queueProvider.getQueue(config.getName()));
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        // In-memory transport requires no additional configuration
    }
}
