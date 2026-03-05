package com.github.vovten.eventflow.publisher;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventBus;

import java.util.Map;

/**
 * Composite event publisher.
 * Designed for sending events to different buses.
 *
 * @author Vladimir Aleshkov, 30.12.2024.
 */
public class CompositeEventPublisher implements EventPublisher {
    private final Map<EventBus, EventPublisher> eventPublishers;
    private final boolean transactionalPublishingEnabled;

    public CompositeEventPublisher(Map<EventBus, EventPublisher> eventPublishers, boolean transactionalPublishingEnabled) {
        this.eventPublishers = eventPublishers;
        this.transactionalPublishingEnabled = transactionalPublishingEnabled;
    }

    @Override
    public void publish(Event event) {
        for (EventBus eventBus : event.eventBusTypes()) {
            EventPublisher publisher = eventPublishers.get(eventBus);
            if (publisher == null) {
                throw new IllegalArgumentException("No publisher found for bus: " + eventBus);
            }
            // Check if transaction is active
            if (transactionalPublishingEnabled && TransactionSynchronizationManager.isActualTransactionActive()) {
                // Register synchronization that will execute after commit
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        publisher.publish(event);
                    }
                });
            } else {
                // No transaction, publish immediately
                publisher.publish(event);
            }
        }


    }

    @Override
    public EventBus eventBus() {
        throw new UnsupportedOperationException();
    }
}
