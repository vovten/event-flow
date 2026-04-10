package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.SendResult;
import com.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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

    private static final Logger log = LoggerFactory.getLogger(AbstractEventChannel.class);

    private final List<OutTransport> transports;

    /**
     * Creates an event channel with multiple transports.
     *
     * @param transports list of transports for this channel
     * @throws IllegalArgumentException if transports is null or empty
     */
    protected AbstractEventChannel(List<OutTransport> transports) {
        validateTransports(transports);
        this.transports = transports;
    }

    /**
     * Creates an event channel with a single transport.
     *
     * @param transport the transport for this channel
     * @throws IllegalArgumentException if transport is null
     */
    protected AbstractEventChannel(OutTransport transport) {
        validateTransport(transport);
        this.transports = List.of(transport);
    }

    @Override
    public List<OutTransport> transports() {
        return transports;
    }

    @Override
    public CompletableFuture<SendResults> send(Event event) {
        String eventName = event.type().getSimpleName();
        String channelName = name();

        List<CompletableFuture<SendResult>> futures = initiateSendToAllTransports(event);

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .handle((ignored, throwable) ->
                        collectResultsFromAllTransports(futures, eventName, channelName)
                );
    }

    private void validateTransports(List<OutTransport> transports) {
        if (transports == null || transports.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Channel '%s' must have at least one transport", name())
            );
        }
    }

    private void validateTransport(OutTransport transport) {
        if (transport == null) {
            throw new IllegalArgumentException(
                    String.format("Channel '%s' transport must not be null", name())
            );
        }
    }

    private List<CompletableFuture<SendResult>> initiateSendToAllTransports(Event event) {
        return transports.stream()
                .map(transport -> transport.send(event))
                .toList();
    }

    private SendResults collectResultsFromAllTransports(List<CompletableFuture<SendResult>> futures,
                                                        String eventName,
                                                        String channelName) {
        List<SendResult> results = new ArrayList<>();
        for (int i = 0; i < transports.size(); i++) {
            CompletableFuture<SendResult> future = futures.get(i);
            SendResult result = extractResultFromTransport(future, transports.get(i), eventName, channelName);
            results.add(result);
        }
        return SendResults.of(results);
    }

    private SendResult extractResultFromTransport(CompletableFuture<SendResult> future,
                                                  OutTransport transport,
                                                  String eventName,
                                                  String channelName) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            logSendFailure(channelName, eventName, transport, cause);
            return SendResult.failure(transport.name(), cause, cause.getMessage());
        }
    }

    private void logSendFailure(String channelName, String eventName, OutTransport transport, Throwable cause) {
        String msg = "Channel '{}': Failed to send event '{}' via transport '{}': {}";
        log.warn(msg, channelName, eventName, transport.name(), cause.getMessage(), cause);
    }
}