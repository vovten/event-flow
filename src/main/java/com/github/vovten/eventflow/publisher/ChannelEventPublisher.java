package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.transport.EventTransport;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes events to channels and their associated transports.
 *
 * @author Vladimir Aleshkov
 * @since 05.03.2026
 */
@Slf4j
public class ChannelEventPublisher implements EventPublisher {
    
    private final Map<Class<? extends EventChannel>, EventChannel> channels;
    private final boolean transactionalPublishingEnabled;
    
    /**
     * @param channels list of channels (can be created manually or via Spring)
     * @param transactionalPublishingEnabled enable deferred publishing during transactions
     */
    public ChannelEventPublisher(List<EventChannel> channels,  boolean transactionalPublishingEnabled) {
        this.channels = new ConcurrentHashMap<>();
        for (EventChannel channel : channels) {
            this.channels.put(channel.getClass(), channel);
        }
        this.transactionalPublishingEnabled = transactionalPublishingEnabled;
    }
    
    @Override
    public void publish(Event event) {
        for (Class<? extends EventChannel> channelType : event.channels()) {
            EventChannel channel = channels.get(channelType);
            if (channel == null) {
                log.warn("Channel '{}' not found for event {}",
                        channelType.getSimpleName(), event.type());
                continue;
            }
            publishToChannel(channel, event);
        }
    }
    
    private void publishToChannel(EventChannel channel, Event event) {
        if (transactionalPublishingEnabled && isTransactionActive()) {
            registerTransactionSynchronization(() -> channel.send(event));
            log.debug("Transaction active, deferred publishing to channel '{}'", channel.name());
        } else {
            channel.send(event);
        }
    }
    
    /**
     * Check if transaction is active — uses reflection to avoid Spring dependency.
     */
    private boolean isTransactionActive() {
        try {
            Class<?> txManager = Class.forName(
                "org.springframework.transaction.support.TransactionSynchronizationManager"
            );
            java.lang.reflect.Method method = txManager.getMethod("isActualTransactionActive");
            return (Boolean) method.invoke(null);
        } catch (Exception e) {
            // Spring not on classpath — no transactions
            return false;
        }
    }
    
    /**
     * Register transaction synchronization — uses reflection to avoid Spring dependency.
     */
    private void registerTransactionSynchronization(Runnable afterCommit) {
        try {
            Class<?> txManager = Class.forName(
                "org.springframework.transaction.support.TransactionSynchronizationManager"
            );
            Class<?> sync = Class.forName(
                "org.springframework.transaction.support.TransactionSynchronization"
            );
            
            Object synchronization = java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] { sync },
                (proxy, method, args) -> {
                    if ("afterCommit".equals(method.getName())) {
                        afterCommit.run();
                    }
                    return null;
                }
            );
            
            java.lang.reflect.Method registerMethod = txManager.getMethod(
                "registerSynchronization", sync
            );
            registerMethod.invoke(null, synchronization);
        } catch (Exception e) {
            // Spring not available — execute immediately
            afterCommit.run();
        }
    }
}
