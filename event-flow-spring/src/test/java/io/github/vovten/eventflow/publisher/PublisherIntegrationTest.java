package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.test.TestEvent;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for EventPublisher with all decorators.
 */
@DisplayName("EventPublisher Integration Tests")
class PublisherIntegrationTest {

    @BeforeEach
    void setUp() {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.initSynchronization();
        }
    }

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clear();
        }
    }

    @Test
    @DisplayName("Should publish successfully with all decorators and active transaction")
    void shouldPublishSuccessfullyWithAllDecoratorsAndActiveTransaction() {
        AtomicBoolean sendCalled = new AtomicBoolean(false);
        OutTransport transport = createMockTransport(sendCalled);
        InternalEventChannel channel = new InternalEventChannel(transport);

        EventPublisher publisher = SpringEventPublisherBuilder.create(channel)
                .retryable(3, Duration.ofMillis(100), 2.0)
                .transactional()
                .build();

        TestEvent event = TestEvent.create("Integration test event");

        TransactionSynchronizationManager.setActualTransactionActive(true);

        try {
            CompletableFuture<SendResults> future = publisher.publish(event);

            assertThat(sendCalled.get()).isFalse();

            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCommit();
            }

            SendResults results = future.join();

            assertThat(sendCalled.get()).isTrue();
            assertThat(results.isAllSuccess()).isTrue();
        } finally {
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("Should not publish event when transaction is rolled back")
    void shouldNotPublishEventWhenTransactionRolledBack() {
        AtomicBoolean sendCalled = new AtomicBoolean(false);
        OutTransport transport = createMockTransport(sendCalled);
        InternalEventChannel channel = new InternalEventChannel(transport);

        EventPublisher publisher = SpringEventPublisherBuilder.create(channel)
                .retryable(3, Duration.ofMillis(100), 2.0)
                .transactional()
                .build();

        TestEvent event = TestEvent.create("Rollback test event");

        TransactionSynchronizationManager.setActualTransactionActive(true);

        try {
            publisher.publish(event);

            assertThat(sendCalled.get()).isFalse();

            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
            }

            assertThat(sendCalled.get()).isFalse();
        } finally {
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    private OutTransport createMockTransport(AtomicBoolean sendCalled) {
        return new OutTransport() {
            @Override
            public String name() {
                return "test-transport";
            }

            @Override
            public CompletableFuture<SendResult> send(Event event) {
                sendCalled.set(true);
                return CompletableFuture.completedFuture(SendResult.success("test-destination"));
            }
        };
    }
}