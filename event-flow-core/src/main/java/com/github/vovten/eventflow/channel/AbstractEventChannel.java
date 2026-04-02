package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.OutTransport;

import java.util.List;

/**
 * Abstract base class for event channels providing common functionality.
 * <p>
 * This class implements the shared behavior across all channel types,
 * including transport management and event sending logic. Concrete channel
 * implementations should extend this class and provide their specific
 * channel name.
 * <p>
 * The abstract class consolidates the common code that was previously
 * duplicated across BroadcastEventChannel, ExternalEventChannel, and
 * InternalEventChannel, reducing code duplication and improving maintainability.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-12
 */
public abstract class AbstractEventChannel implements EventChannel {

    private final List<OutTransport> transports;

    /**
     * Creates an event channel with multiple transports.
     *
     * @param transports list of transports for this channel
     */
    protected AbstractEventChannel(List<OutTransport> transports) {
        this.transports = transports;
    }

    /**
     * Creates an event channel with a single transport.
     *
     * @param transport the transport for this channel
     */
    protected AbstractEventChannel(OutTransport transport) {
        this.transports = List.of(transport);
    }

    @Override
    public List<OutTransport> transports() {
        return transports;
    }

    @Override
    public void send(Event event) {
        for (OutTransport transport : transports()) {
            transport.send(event);
        }
    }
}
