package com.github.vovten.eventflow.autoconfig.transport.incoming;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.InTransportFactory;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import com.github.vovten.eventflow.transport.InTransport;
import com.github.vovten.eventflow.transport.incoming.LocalQueueInTransport;

/**
 * Factory for creating in-memory dispatcher event transports.
 * <p>
 * Uses a shared {@link DefaultLocalQueueProvider} for all in-memory transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class LocalQueueInTransportFactory implements InTransportFactory {

    private final DefaultLocalQueueProvider queueProvider;

    public LocalQueueInTransportFactory(DefaultLocalQueueProvider queueProvider) {
        this.queueProvider = queueProvider;
    }

    @Override
    public String getType() {
        return "in-memory";
    }

    @Override
    public InTransport createDispatcher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new LocalQueueInTransport(queueProvider.getQueue(config.getName()));
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        // In-memory transport requires no additional configuration
    }
}
