package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.InMemoryTransportsBuilder;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;

/**
 * Transport factory for in-memory event transports.
 * <p>
 * Uses shared queues to couple publisher and dispatcher for efficient
 * internal event communication.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@Component
@RequiredArgsConstructor
public class InMemoryTransportFactory implements TransportFactory {
    
    private final ExecutorService eventFlowExecutor;
    private final InMemoryTransportsBuilder.InMemoryTransports defaultTransports;
    
    @Override
    public String getType() {
        return "in-memory";
    }
    
    @Override
    public OutgoingEventTransport createOutgoing(EventFlowProperties.ChannelConfig config) {
        // For internal channel, use the shared queue (couples publisher/dispatcher)
        if ("internal".equalsIgnoreCase(config.getName())) {
            return defaultTransports.outgoing();
        }
        
        // For custom in-memory channels, create new independent transports
        return new InMemoryTransportsBuilder()
            .queueSize(config.getCapacity())
            .executorService(eventFlowExecutor)
            .build()
            .outgoing();
    }
    
    @Override
    public IncomingEventTransport createIncoming(EventFlowProperties.TransportConfig config) {
        // Create new independent transport for incoming events
        return new InMemoryTransportsBuilder()
            .queueSize(config.getCapacity())
            .executorService(eventFlowExecutor)
            .build()
            .incoming();
    }
}
