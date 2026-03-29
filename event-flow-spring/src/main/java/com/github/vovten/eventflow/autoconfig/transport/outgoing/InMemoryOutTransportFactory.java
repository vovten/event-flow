package com.github.vovten.eventflow.autoconfig.transport.outgoing;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;

/**
 * Factory for creating in-memory publisher event transports.
 * <p>
 * Uses a shared {@link DefaultLocalQueueProvider} for all in-memory transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class InMemoryOutTransportFactory implements OutTransportFactory {

    private final DefaultLocalQueueProvider queueProvider;

    public InMemoryOutTransportFactory(DefaultLocalQueueProvider queueProvider) {
        this.queueProvider = queueProvider;
    }

    @Override
    public String getName() {
        return "in-memory";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new LocalQueueOutTransport(queueProvider.getQueue(config.getName()));
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        // In-memory transport requires no additional configuration
    }
}
