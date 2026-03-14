package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Decorator for {@link EventPublisher} that adds transactional publishing support.
 * <p>
 * When a transaction is active, event publishing is deferred until after the transaction commits.
 * This ensures that events are only published if the transaction succeeds.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * EventPublisher basePublisher = new ChannelEventPublisher(channels);
 * EventPublisher retryPublisher = new RetryEventPublisherDecorator(basePublisher);
 * EventPublisher transactionalPublisher = new TransactionalEventPublisher(retryPublisher);
 * transactionalPublisher.publish(event);  // Retry + deferred until after commit
 * }</pre>
 * <p>
 * <b>How it works:</b>
 * <ol>
 *   <li>Checks if Spring transaction is active via {@code TransactionSynchronizationManager}</li>
 *   <li>If active, registers {@code TransactionSynchronization} to publish after commit</li>
 *   <li>If not active, publishes immediately</li>
 * </ol>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 * @see ChannelEventPublisher
 * @see TransactionSynchronizationManager
 */
@Slf4j
public class TransactionalEventPublisher implements EventPublisher {

    private final EventPublisher origin;

    /**
     * Create transactional decorator
     *
     * @param origin the delegate publisher to wrap
     */
    public TransactionalEventPublisher(EventPublisher origin) {
        this.origin = origin;
    }

    @Override
    public void publish(Event event) {
        if (isTransactionActive()) {
            registerTransactionSynchronization(() -> origin.publish(event));
            log.debug("Transaction active, deferred publishing for event {}", event.type());
        } else {
            origin.publish(event);
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
