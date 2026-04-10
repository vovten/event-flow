package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.transport.SendResult;
import com.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event publisher that routes events to channels and their associated transports.
 * <p>
 * This is the core publisher implementation that handles event routing based on the
 * channels specified in each event. It maintains a registry of configured channels
 * and delegates event delivery to the appropriate channel's transports.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Pure Java implementation — no Spring dependency</li>
 *   <li>Thread-safe channel registry using {@code ConcurrentHashMap}</li>
 *   <li>Strict validation — throws exception if required channel is not configured</li>
 *   <li>Supports multiple channels per event</li>
 *   <li>Asynchronous publishing with CompletableFuture</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>{@code
 * Event → ChannelEventPublisher → EventChannel → OutgoingEventTransport(s)
 *                                      ↓
 *                              KafkaTransport, LocalQueueTransport, etc.
 * }</pre>
 * <p>
 * <b>Usage example (manual configuration):</b>
 * <pre>{@code
 * // Create channels with their transports
 * EventChannel internalChannel = new InternalEventChannel(
 *     List.of(new LocalQueueOutgoingEventTransport(1000))
 * );
 * EventChannel externalChannel = new ExternalEventChannel(
 *     List.of(new KafkaOutgoingEventTransport(bootstrapServers, "events"))
 * );
 *
 * // Create publisher with configured channels
 * EventPublisher publisher = new ChannelEventPublisher(channels);
 *
 * // Publish event
 * publisher.publish(new OrderCreatedEvent("order-123"))
 *     .thenAccept(results -> log.info("Published to {} destinations", results.size()))
 *     .exceptionally(ex -> { log.error("Failed", ex); return null; });
 * }</pre>
 * <p>
 * <b>Usage example (with Spring):</b>
 * For Spring integration with transaction support, use the event-flow-spring module.
 * <pre>{@code
 * @Configuration
 * public class EventConfig {
 *
 *     @Bean
 *     public EventPublisher eventPublisher(List<EventChannel> channels) {
 *         return new ChannelEventPublisher(channels);
 *     }
 * }
 * }</pre>
 * <p>
 * <b>Retry support:</b>
 * For automatic retry on transient failures, wrap this publisher with
 * {@link RetryEventPublisher}.
 * <p>
 * <b>Transaction support:</b>
 * For transactional event publishing (defer until after commit), use the
 * event-flow-spring module which provides {@code TransactionalEventPublisher}.
 * <p>
 * <b>Error handling:</b>
 * If an event specifies a channel that is not configured in the system,
 * {@code EventPublisherException} is thrown with a detailed error message.
 * This ensures that misconfigurations are detected early.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 * @see EventChannel
 * @see RetryEventPublisher
 */
public class ChannelEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ChannelEventPublisher.class);

    private final Map<Class<? extends EventChannel>, EventChannel> channels;

    /**
     * Create channel-based event publisher.
     *
     * @param channels list of event channels to route events through
     * @throws IllegalArgumentException if channels list is empty
     */
    public ChannelEventPublisher(List<EventChannel> channels) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("At least one channel must be configured");
        }
        this.channels = new ConcurrentHashMap<>();
        for (EventChannel channel : channels) {
            this.channels.put(channel.getClass(), channel);
        }
    }

    /**
     * Publish event to all channels specified by the event.
     * <p>
     * For each channel class returned by {@code event.channels()}, this method:
     * <ol>
     *   <li>Looks up the configured channel from the registry</li>
     *   <li>Throws {@code EventPublisherException} if channel is not found</li>
     *   <li>Delegates to {@code channel.send(event)} for delivery</li>
     * </ol>
     * <p>
     * Events are published sequentially to each channel. All sends are asynchronous.
     *
     * @param event the event to publish
     * @return CompletableFuture that completes with list of SendResults from all channels
     * @throws EventPublisherConfigException if required channel is not configured
     */
    @Override
    public CompletableFuture<SendResults> publish(Event event) {
        try {
            List<CompletableFuture<SendResults>> channelFutures = new ArrayList<>();
            for (Class<? extends EventChannel> channelType : event.channels()) {
                EventChannel channel = channels.get(channelType);
                checkChannel(event, channelType, channel);
                channelFutures.add(channel.send(event));
            }
            return CompletableFuture.allOf(channelFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        List<SendResult> allResults = channelFutures.stream()
                                .flatMap(future -> future.join().asList().stream())
                                .toList();
                        return SendResults.of(allResults);
                    });
        } catch (EventPublisherConfigException ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    private void checkChannel(Event event, Class<? extends EventChannel> channelType, EventChannel channel) {
        if (channel == null) {
            String text = """
                    Channel '%s' required for event %s but not configured in the system.
                    Check that the channel bean is created and registered.""";
            String msg = String.format(text, channelType.getSimpleName(), event.type().getSimpleName());
            throw new EventPublisherConfigException(msg);
        }
    }
}
