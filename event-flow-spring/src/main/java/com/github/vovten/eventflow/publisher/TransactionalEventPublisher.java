package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Decorator for {@link EventPublisher} that adds transactional event publishing support.
 * <p>
 * When a transaction is active, event publishing is deferred until after the transaction commits.
 * This ensures that events are only published if the transaction succeeds.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * EventPublisher basePublisher = new ChannelEventPublisher(channels);
 * EventPublisher transactionalPublisher = new TransactionalEventPublisher(basePublisher);
 * transactionalPublisher.publish(event)
 *     .thenAccept(results -> log.info("Published to {} destinations", results.size()));
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
public class TransactionalEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(TransactionalEventPublisher.class);

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
    public CompletableFuture<List<SendResult>> publish(Event event) {
        if (isTransactionActive()) {
            CompletableFuture<List<SendResult>> resultFuture = new CompletableFuture<>();
            registerTransactionSynchronization(() ->
                    origin.publish(event)
                            .thenAccept(resultFuture::complete)
                            .exceptionally(ex -> {
                                resultFuture.completeExceptionally(ex);
                                return null;
                            })
            );
            log.debug("Transaction active, deferred publishing for event {}", event.type().getSimpleName());
            return resultFuture;
        } else {
            return origin.publish(event);
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
