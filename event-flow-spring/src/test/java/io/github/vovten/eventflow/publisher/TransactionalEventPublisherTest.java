package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.TestEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for the transactional publisher failure handling.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("TransactionalEventPublisher Failure Tests")
class TransactionalEventPublisherTest {

    @BeforeEach
    void setUp() {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.initSynchronization();
        }
    }

    @AfterEach
    void tearDown() {
        TransactionSynchronizationManager.setActualTransactionActive(false);
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    @DisplayName("DEFECT-1.1: Should complete the deferred future exceptionally when afterCommit throws synchronously")
    void shouldCompleteFutureExceptionallyWhenDeferredPublicationThrowsSynchronously() {
        // origin.publish() throws before it can return a future: the deferred
        // thenAccept/exceptionally chain never runs.
        EventPublisher origin = event -> {
            throw new IllegalStateException("synchronous publication failure");
        };
        TransactionalEventPublisher publisher = new TransactionalEventPublisher(origin);
        TestEvent event = TestEvent.create("deferred sync failure");

        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            CompletableFuture<SendResults> future = publisher.publish(event);
            assertThat(future.isDone()).isFalse();

            TransactionSynchronization synchronization =
                    TransactionSynchronizationManager.getSynchronizations().getFirst();

            // Spring swallows exceptions from afterCommit (it only logs them),
            // so the lookup must not leak the exception to the transaction manager.
            assertThatCode(synchronization::afterCommit).doesNotThrowAnyException();

            // Deferred publication runs outside the transaction thread.
            future.exceptionally(error -> null).join();

            assertThat(future.isDone())
                    .as("The future must not hang when the deferred publication throws synchronously")
                    .isTrue();
            assertThat(future.isCompletedExceptionally()).isTrue();
        } finally {
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("DEFECT-1.2: Should return a failed future when immediate publication throws synchronously")
    void shouldReturnFailedFutureWhenImmediatePublicationThrowsSynchronously() {
        EventPublisher origin = event -> {
            throw new IllegalStateException("synchronous publication failure");
        };
        TransactionalEventPublisher publisher = new TransactionalEventPublisher(origin);
        TestEvent event = TestEvent.create("immediate sync failure");

        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();

        AtomicReference<CompletableFuture<SendResults>> captured = new AtomicReference<>();
        assertThatCode(() -> captured.set(publisher.publish((Event) event)))
                .as("A synchronous origin failure must be delivered through the future, not thrown")
                .doesNotThrowAnyException();

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().isCompletedExceptionally()).isTrue();
    }
}
