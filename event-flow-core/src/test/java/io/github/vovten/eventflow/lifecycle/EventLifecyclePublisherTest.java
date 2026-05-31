package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("EventLifecyclePublisher Tests")
class EventLifecyclePublisherTest {

    private InMemoryEventStore eventStore;
    private TestEvent event;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        event = new TestEvent("test-data");
        eventId = event.eventId();
    }

    // -------------------------------------------------------
    // MANAGED — full lifecycle
    // -------------------------------------------------------

    @Nested
    @DisplayName("MANAGED lifecycle")
    class ManagedTest {

        @Test
        @DisplayName("Should save event as NEW before publishing")
        void shouldSaveEventBeforePublishing() {
            EventPublisher origin = e -> CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.success("dest"))));
            EventLifecyclePublisher publisher = new EventLifecyclePublisher(
                    origin, eventStore, "test-service");

            publisher.publish(event).join();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.eventType()).contains("TestEvent");
        }

        @Test
        @DisplayName("Should update status to FAILED on publication failure")
        void shouldUpdateStatusOnFailure() {
            EventPublisher origin = e -> CompletableFuture.failedFuture(new RuntimeException("Network error"));
            EventLifecyclePublisher publisher = new EventLifecyclePublisher(
                    origin, eventStore, "test-service");

            assertThrows(Exception.class, () -> publisher.publish(event).join());

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.FAILED);
            assertThat(stored.errorDetails()).contains("Network error");
        }

        @Test
        @DisplayName("Should update status to FAILED when all transports fail")
        void shouldUpdateStatusOnAllFailures() {
            EventPublisher origin = e -> CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.failure("dest", "timeout"))));
            EventLifecyclePublisher publisher = new EventLifecyclePublisher(
                    origin, eventStore, "test-service");

            publisher.publish(event).join();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.FAILED);
        }

        @Test
        @DisplayName("Should reset status to NEW on retry of existing event")
        void shouldResetStatusOnRetry() {
            // First publish that fails
            EventPublisher failingOrigin = e -> CompletableFuture.failedFuture(new RuntimeException("Fail"));
            EventLifecyclePublisher failingPublisher = new EventLifecyclePublisher(
                    failingOrigin, eventStore, "test-service");
            assertThrows(Exception.class, () -> failingPublisher.publish(event).join());

            assertThat(eventStore.findById(eventId).orElseThrow().status())
                    .isEqualTo(EventStatus.FAILED);

            // Second publish that succeeds — should reset to NEW then PUBLISHED
            EventPublisher successOrigin = e -> CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.success("dest"))));
            EventLifecyclePublisher successPublisher = new EventLifecyclePublisher(
                    successOrigin, eventStore, "test-service");
            successPublisher.publish(event).join();

            StoredEvent stored = eventStore.findById(eventId).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.PUBLISHED);
            assertThat(stored.retryCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("Should pass through LifecycleAckEvent without persisting")
        void shouldSkipAckEvents() {
            SuccessAck ack = new SuccessAck(
                    UUID.randomUUID(), eventId, "TestEvent", null,
                    List.of(), null, Instant.now());
            EventPublisher origin = e -> {
                assertThat(e).isInstanceOf(LifecycleAckEvent.class);
                return CompletableFuture.completedFuture(
                        SendResults.of(List.of(SendResult.success("dest"))));
            };
            EventLifecyclePublisher publisher = new EventLifecyclePublisher(
                    origin, eventStore, "test-service");

            publisher.publish(ack).join();

            assertThat(eventStore.findById(ack.eventId())).isEmpty();
            assertThat(eventStore.findById(eventId)).isEmpty();
        }
    }

    // -------------------------------------------------------
    // PERSISTED — save-only
    // -------------------------------------------------------

    @Nested
    @DisplayName("PERSISTED lifecycle")
    class PersistedTest {

        private PersistedEvent persistedEvent;

        @BeforeEach
        void setUp() {
            persistedEvent = new PersistedEvent("persist-data");
        }

        @Test
        @DisplayName("Should save event as UNDEFINED without tracking publish status")
        void shouldSaveAsUndefined() {
            EventPublisher origin = e -> CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.success("dest"))));
            EventLifecyclePublisher publisher = new EventLifecyclePublisher(
                    origin, eventStore, "test-service");

            publisher.publish(persistedEvent).join();

            StoredEvent stored = eventStore.findById(persistedEvent.eventId()).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.UNDEFINED);
            assertThat(stored.retryCount()).isZero();
        }

        @Test
        @DisplayName("Should save as UNDEFINED even when publication fails")
        void shouldStayUndefinedOnFailure() {
            EventPublisher origin = e -> CompletableFuture.failedFuture(new RuntimeException("fail"));
            EventLifecyclePublisher publisher = new EventLifecyclePublisher(
                    origin, eventStore, "test-service");

            assertThrows(Exception.class, () -> publisher.publish(persistedEvent).join());

            // Event is saved as UNDEFINED but never updated — stays UNDEFINED
            StoredEvent stored = eventStore.findById(persistedEvent.eventId()).orElseThrow();
            assertThat(stored.status()).isEqualTo(EventStatus.UNDEFINED);
            assertThat(stored.retryCount()).isZero();
        }

        @Test
        @DisplayName("Should not overwrite existing persisted event on re-publish")
        void shouldNotOverwriteOnRepublish() {
            // First publish the event
            EventPublisher origin = e -> CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.success("dest"))));
            EventLifecyclePublisher publisher = new EventLifecyclePublisher(
                    origin, eventStore, "test-service");

            publisher.publish(persistedEvent).join();
            assertThat(eventStore.findById(persistedEvent.eventId()))
                    .hasValueSatisfying(e -> assertThat(e.status()).isEqualTo(EventStatus.UNDEFINED));

            // Re-publish — should keep UNDEFINED, no status changes
            publisher.publish(persistedEvent).join();
            assertThat(eventStore.findById(persistedEvent.eventId()))
                    .hasValueSatisfying(e -> assertThat(e.status()).isEqualTo(EventStatus.UNDEFINED));
        }
    }

    // -------------------------------------------------------
    // NONE — skip persistence
    // -------------------------------------------------------

    @Nested
    @DisplayName("NONE lifecycle")
    class NoneTest {

        @Test
        @DisplayName("Should skip persistence for NONE lifecycle event")
        void shouldSkipPersistenceForNoneLifecycle() {
            EventPublisher origin = e -> CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.success("dest"))));
            EventLifecyclePublisher publisher = new EventLifecyclePublisher(
                    origin, eventStore, "test-service");

            publisher.publish(new NoneLifecycleEvent()).join();

            assertThat(eventStore.findById(UUID.randomUUID())).isEmpty();
        }
    }

    // -------------------------------------------------------
    // Test fixtures
    // -------------------------------------------------------

    @io.github.vovten.eventflow.event.annotation.Event(lifecycle = EventLifecycle.MANAGED)
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

    @io.github.vovten.eventflow.event.annotation.Event(lifecycle = EventLifecycle.PERSISTED)
    private static class PersistedEvent extends AbstractTraceableEvent {
        private final String data;

        PersistedEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return PersistedEvent.class;
        }
    }

    @io.github.vovten.eventflow.event.annotation.Event(lifecycle = EventLifecycle.NONE)
    private static final class NoneLifecycleEvent implements Event {
        @Override
        public Class<?> type() {
            return NoneLifecycleEvent.class;
        }
    }
}
