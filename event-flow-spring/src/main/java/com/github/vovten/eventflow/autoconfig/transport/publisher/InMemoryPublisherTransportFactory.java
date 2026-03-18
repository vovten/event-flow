package com.github.vovten.eventflow.autoconfig.transport.publisher;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.PublisherTransportFactory;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import com.github.vovten.eventflow.transport.PublisherTransport;
import com.github.vovten.eventflow.transport.publisher.InMemoryPublisherTransport;

/**
 * Factory for creating in-memory publisher event transports.
 * <p>
 * Uses a shared {@link DefaultQueueProvider} for all in-memory transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-11
 */
public class InMemoryPublisherTransportFactory implements PublisherTransportFactory {

    private final DefaultQueueProvider queueProvider;

    public InMemoryPublisherTransportFactory(DefaultQueueProvider queueProvider) {
        this.queueProvider = queueProvider;
    }

    @Override
    public String getName() {
        return "in-memory";
    }

    @Override
    public PublisherTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new InMemoryPublisherTransport(queueProvider.getQueue(config.getName()));
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        // In-memory transport requires no additional configuration
    }
}
