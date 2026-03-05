package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.channel.EventChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes events to channels and their associated transports.
 * Does NOT depend on Spring — pure Java implementation.
 * For transactional support, wrap with {@link TransactionalEventPublisherDecorator}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
@Slf4j
public class ChannelEventPublisher implements EventPublisher {

    private final Map<Class<? extends EventChannel>, EventChannel> channels;

    /**
     * @param channels list of channels (can be created manually or via Spring)
     */
    public ChannelEventPublisher(List<EventChannel> channels) {
        this.channels = new ConcurrentHashMap<>();
        for (EventChannel channel : channels) {
            this.channels.put(channel.getClass(), channel);
        }
    }

    @Override
    public void publish(Event event) {
        for (Class<? extends EventChannel> channelType : event.channels()) {
            EventChannel channel = channels.get(channelType);
            if (channel == null) {
                log.warn("Channel '{}' not found for event {}", channelType.getSimpleName(), event.type());
                continue;
            }
            channel.send(event);
        }
    }
}
