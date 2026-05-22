package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.lifecycle.SuccessAck;
import io.github.vovten.eventflow.event.lifecycle.LifecycleAckEvent;
import io.github.vovten.eventflow.store.EventStatus;
import io.github.vovten.eventflow.store.InMemoryEventStore;
import io.github.vovten.eventflow.store.StoredEvent;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PersistentEventPublisher Tests")
class PersistentEventPublisherTest {

    private InMemoryEventStore eventStore;
    private TestEvent event;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        event = new TestEvent("test-data");
        eventId = event.eventId();
    }

    @Test
    @DisplayName("Should save event before publishing on first attempt")
    void shouldSaveEventBeforePublishing() {
        EventPublisher origin = e -> CompletableFuture.completedFuture(
                SendResults.of(List.of(SendResult.success("dest"))));
        PersistentEventPublisher publisher = new PersistentEventPublisher(origin, eventStore, "test-service");

        publisher.publish(event).join();

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
        assertThat(stored.eventType()).contains("TestEvent");
    }

    @Test
    @DisplayName("Should update status to PUBLISH_FAILED on publication failure")
    void shouldUpdateStatusOnFailure() {
        EventPublisher origin = e -> CompletableFuture.failedFuture(new RuntimeException("Network error"));
        PersistentEventPublisher publisher = new PersistentEventPublisher(origin, eventStore, "test-service");

        assertThrows(Exception.class, () -> publisher.publish(event).join());

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.PUBLISH_FAILED);
        assertThat(stored.errorDetails()).contains("Network error");
    }

    @Test
    @DisplayName("Should update status to PUBLISH_FAILED when all transports fail")
    void shouldUpdateStatusOnAllFailures() {
        EventPublisher origin = e -> CompletableFuture.completedFuture(
                SendResults.of(List.of(SendResult.failure("dest", "timeout"))));
        PersistentEventPublisher publisher = new PersistentEventPublisher(origin, eventStore, "test-service");

        publisher.publish(event).join();

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.PUBLISH_FAILED);
    }

    @Test
    @DisplayName("Should skip persistence for LifecycleAckEvent")
    void shouldSkipAckEvents() {
        SuccessAck ack = new SuccessAck(
                UUID.randomUUID(), eventId, "TestEvent", null,
                List.of(), null, Instant.now());
        EventPublisher origin = e -> {
            assertThat(e).isInstanceOf(LifecycleAckEvent.class);
            return CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.success("dest"))));
        };
        PersistentEventPublisher publisher = new PersistentEventPublisher(origin, eventStore, "test-service");

        publisher.publish(ack).join();

        // No event should be stored for the ack
        assertThat(eventStore.findById(ack.eventId())).isEmpty();
        // The original event should remain unchanged
        assertThat(eventStore.findById(eventId)).isEmpty();
    }

    @Test
    @DisplayName("Should reset status to NEW on retry of existing event")
    void shouldResetStatusOnRetry() {
        // First publish that fails
        EventPublisher failingOrigin = e -> CompletableFuture.failedFuture(new RuntimeException("Fail"));
        PersistentEventPublisher failingPublisher = new PersistentEventPublisher(failingOrigin, eventStore, "test-service");
        assertThrows(Exception.class, () -> failingPublisher.publish(event).join());

        assertThat(eventStore.findById(eventId).orElseThrow().status())
                .isEqualTo(EventStatus.PUBLISH_FAILED);

        // Second publish that succeeds — should reset to NEW then PUBLISHED
        EventPublisher successOrigin = e -> CompletableFuture.completedFuture(
                SendResults.of(List.of(SendResult.success("dest"))));
        PersistentEventPublisher successPublisher = new PersistentEventPublisher(successOrigin, eventStore, "test-service");
        successPublisher.publish(event).join();

        StoredEvent stored = eventStore.findById(eventId).orElseThrow();
        assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
        assertThat(stored.retryCount()).isEqualTo(1);
    }

    /**
     * Test event that extends AbstractTraceableEvent.
     */
    private static class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        public String getData() {
            return data;
        }
    }
}
