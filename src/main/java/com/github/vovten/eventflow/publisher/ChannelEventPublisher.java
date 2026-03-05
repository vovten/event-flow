package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.channel.EventChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
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
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>{@code
 * Event → ChannelEventPublisher → EventChannel → EventTransport(s)
 *                                      ↓
 *                              KafkaTransport, InMemoryTransport, etc.
 * }</pre>
 * <p>
 * <b>Usage example (manual configuration):</b>
 * <pre>{@code
 * // Create channels with their transports
 * EventChannel internalChannel = new InternalEventChannel(
 *     List.of(new InMemoryEventTransport(1000))
 * );
 * EventChannel externalChannel = new ExternalEventChannel(
 *     List.of(new KafkaEventTransport(bootstrapServers, "events"))
 * );
 *
 * // Create publisher with configured channels
 * EventPublisher publisher = new ChannelEventPublisher(
 *     List.of(internalChannel, externalChannel)
 * );
 *
 * // Publish event
 * publisher.publish(new OrderCreatedEvent("order-123"));
 * }</pre>
 * <p>
 * <b>Usage example (with Spring):</b>
 * <pre>{@code
 * @Configuration
 * public class EventConfig {
 *
 *     @Bean
 *     public EventPublisher eventPublisher(List<EventChannel> channels) {
 *         EventPublisher basePublisher = new ChannelEventPublisher(channels);
 *         return new TransactionalEventPublisherDecorator(basePublisher);
 *     }
 * }
 * }</pre>
 * <p>
 * <b>Transaction support:</b>
 * For transactional event publishing (defer until after commit), wrap this publisher
 * with {@link TransactionalEventPublisherDecorator}.
 * <p>
 * <b>Error handling:</b>
 * If an event specifies a channel that is not configured in the system,
 * {@code IllegalStateException} is thrown with a detailed error message.
 * This ensures that misconfigurations are detected early.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 * @see EventChannel
 * @see TransactionalEventPublisherDecorator
 */
@Slf4j
public class ChannelEventPublisher implements EventPublisher {

    private static final String ERROR_MSG = """
            Channel '%s' required for event %s but not configured in the system.
            Check that the channel bean is created and registered.""";
    
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
     *   <li>Throws {@code IllegalStateException} if channel is not found</li>
     *   <li>Delegates to {@code channel.send(event)} for delivery</li>
     * </ol>
     * <p>
     * Events are published sequentially to each channel. If a channel fails,
     * the exception propagates to the caller and remaining channels are not processed.
     *
     * @param event the event to publish
     * @throws IllegalStateException if required channel is not configured
     * @throws RuntimeException if channel send fails
     */
    @Override
    public void publish(Event event) {
        for (Class<? extends EventChannel> channelType : event.channels()) {
            EventChannel channel = channels.get(channelType);
            if (channel == null) {
                throw new IllegalStateException(String.format(
                        ERROR_MSG,
                        channelType.getSimpleName(),
                        event.type().getSimpleName()
                ));
            }
            channel.send(event);
        }
    }
}
