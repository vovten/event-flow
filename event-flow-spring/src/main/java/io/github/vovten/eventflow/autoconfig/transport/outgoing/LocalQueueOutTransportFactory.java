package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import io.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;

/**
 * Factory for creating local-queue publisher event transports.
 * <p>
 * Uses a shared {@link DefaultLocalQueueProvider} for all local-queue transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class LocalQueueOutTransportFactory implements OutTransportFactory {

    private final DefaultLocalQueueProvider queueProvider;

    public LocalQueueOutTransportFactory(DefaultLocalQueueProvider queueProvider) {
        this.queueProvider = queueProvider;
    }

    @Override
    public String getName() {
        return "local-queue";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new LocalQueueOutTransport(queueProvider.getQueue(config.getName()));
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        // Local-queue transport requires no additional configuration
    }
}
