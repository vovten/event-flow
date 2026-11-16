package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
 *   <li>After commit, the publication runs on a separate thread so that a JDBC
 *       event-store write does not reuse the completed transaction's bound connection;
 *       the saved row becomes visible before the event is delivered</li>
 *   <li>If not active, publishes immediately</li>
 * </ol>
 * <p>
 * <b>Important: Thread safety and transaction boundaries</b>
 * <p>
 * When using inside a {@code @Transactional} method, DO NOT call blocking operations like
 * {@code .join()} or {@code .get()} on the returned Future. This will cause a deadlock because:
 * <ul>
 *   <li>The transaction cannot commit until the method completes</li>
 *   <li>The Future cannot complete until after the transaction commits (via afterCommit)</li>
 * </ul>
 * <p>
 * <b>Incorrect usage (causes deadlock):</b>
 * <pre>{@code
 * @Transactional
 * public void processOrder(Order order) {
 *     orderRepository.save(order);
 *     if (publisher.publish(new OrderCreatedEvent(order)).join().isAllSuccess()) {
 *         outboxService.delete(order.getId());
 *     }
 * }
 * }</pre>
 * <p>
 * <b>Correct usage:</b>
 * <pre>{@code
 * // CORRECT - non-blocking, callback runs AFTER transaction commits
 * publisher.publish(event).thenAccept(results -> {
 *     // This callback executes in a separate thread AFTER transaction commits
 *     if (results.isAllSuccess()) {
 *         outboxService.delete(entity.getId());
 *     }
 * });
 * }</pre>
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see ChannelEventPublisher
 * @see TransactionSynchronizationManager
 */
public class TransactionalEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(TransactionalEventPublisher.class);

    private static final AtomicInteger DEFERRED_THREADS = new AtomicInteger();

    /**
     * Executor for work that must run outside the committing thread.
     * <p>
     * Spring keeps transaction resources bound to the committing thread until
     * {@code afterCompletion}. Publishing on that thread would make a JDBC
     * event-store write reuse the already-completed transaction connection,
     * so the saved row would not be visible to a consumer that acknowledges the
     * event immediately. A separate thread has no transaction context and
     * therefore uses an independent, auto-committed connection.
     */
    private static final Executor DEFERRED_EXECUTOR = Executors.newCachedThreadPool(task -> {
        Thread thread = new Thread(task,
                "event-flow-transactional-publisher-" + DEFERRED_THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });

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
    public CompletableFuture<SendResults> publish(Event event) {
        if (isTransactionActive()) {
            CompletableFuture<SendResults> resultFuture = new CompletableFuture<>();
            registerTransactionSynchronization(resultFuture, () ->
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
            try {
                return origin.publish(event);
            } catch (RuntimeException e) {
                // Keep the "always a CompletableFuture" contract even when the
                // delegate throws before it can return a future.
                return CompletableFuture.failedFuture(
                        new EventPublisherException("Event publication failed synchronously", e));
            }
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
     * <p>
     * The returned future completes with the publish result after a successful
     * commit and completes exceptionally when the transaction rolls back, so
     * callers never block on a future that can never complete.
     *
     * @param resultFuture  the future to complete with the outcome
     * @param publishAction the action to execute after commit
     */
    private void registerTransactionSynchronization(CompletableFuture<SendResults> resultFuture,
                                                    Runnable publishAction) {
        TransactionSynchronizationManager.registerSynchronization(
                new DeferredPublicationSynchronization(resultFuture, publishAction));
    }

    /**
     * Completes the deferred publication future on transaction commit/rollback.
     * <p>
     * Spring swallows exceptions thrown from {@code afterCommit} (it only logs
     * them), so any synchronous publication failure must be translated into an
     * exceptional completion; otherwise the caller would block forever.
     */
    private static final class DeferredPublicationSynchronization implements TransactionSynchronization {

        private final CompletableFuture<SendResults> resultFuture;
        private final Runnable publishAction;

        private DeferredPublicationSynchronization(CompletableFuture<SendResults> resultFuture,
                                                   Runnable publishAction) {
            this.resultFuture = resultFuture;
            this.publishAction = publishAction;
        }

        @Override
        public void afterCommit() {
            DEFERRED_EXECUTOR.execute(() -> {
                try {
                    publishAction.run();
                } catch (Exception e) {
                    log.error("Deferred event publication failed after transaction commit", e);
                    resultFuture.completeExceptionally(
                            new EventPublisherException("Deferred event publication failed", e));
                }
            });
        }

        @Override
        public void afterCompletion(int status) {
            if (status != TransactionSynchronization.STATUS_COMMITTED) {
                resultFuture.completeExceptionally(new IllegalStateException(
                        "Transaction was not committed (status: " + status + "), event publication aborted"));
            }
        }
    }
}
