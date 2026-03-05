package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Decorator for {@link EventPublisher} that adds transactional publishing support.
 * <p>
 * When a transaction is active, event publishing is deferred until after the transaction commits.
 * This ensures that events are only published if the transaction succeeds.
 * <p>
 * Usage example:
 * <pre>{@code
 * EventPublisher basePublisher = new ChannelEventPublisher(channels);
 * EventPublisher transactionalPublisher = new TransactionalEventPublisherDecorator(basePublisher);
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 */
@Slf4j
public class TransactionalEventPublisherDecorator implements EventPublisher {

    private final EventPublisher delegate;

    /**
     * Create transactional decorator.
     *
     * @param delegate the delegate publisher to wrap
     */
    public TransactionalEventPublisherDecorator(EventPublisher delegate) {
        this.delegate = delegate;
    }

    @Override
    public void publish(Event event) {
        if (isTransactionActive()) {
            registerTransactionSynchronization(() -> delegate.publish(event));
            log.debug("Transaction active, deferred publishing for event {}", event.type());
        } else {
            delegate.publish(event);
        }
    }

    /**
     * Check if Spring transaction is active.
     *
     * @return true if transaction is active
     */
    private boolean isTransactionActive() {
        return TransactionSynchronizationManager.isActualTransactionActive();
    }

    /**
     * Register synchronization to publish event after transaction commit.
     *
     * @param publishAction the action to execute after commit
     */
    private void registerTransactionSynchronization(Runnable publishAction) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                publishAction.run();
            }
        });
    }
}
