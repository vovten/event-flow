package com.github.vovten.eventflow.autoconfig.transport.dispatcher;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.DispatcherTransportFactory;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import com.github.vovten.eventflow.transport.DispatcherTransport;
import com.github.vovten.eventflow.transport.dispatcher.InMemoryDispatcherTransport;

/**
 * Factory for creating in-memory dispatcher event transports.
 * <p>
 * Uses a shared {@link DefaultQueueProvider} for all in-memory transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class InMemoryDispatcherTransportFactory implements DispatcherTransportFactory {

    private final DefaultQueueProvider queueProvider;

    public InMemoryDispatcherTransportFactory(DefaultQueueProvider queueProvider) {
        this.queueProvider = queueProvider;
    }

    @Override
    public String getType() {
        return "in-memory";
    }

    @Override
    public DispatcherTransport createDispatcher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new InMemoryDispatcherTransport(queueProvider.getQueue(config.getName()));
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        // In-memory transport requires no additional configuration
    }
}
