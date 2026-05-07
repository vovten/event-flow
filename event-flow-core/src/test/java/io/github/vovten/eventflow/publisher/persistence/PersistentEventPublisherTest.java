package io.github.vovten.eventflow.publisher.persistence;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("PersistentEventPublisher Tests")
class PersistentEventPublisherTest {

    @Mock
    private EventPublisher origin;

    @Mock
    private EventRepository repository;

    private PersistentEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new PersistentEventPublisher(origin, repository);
    }

    @Test
    @DisplayName("Should save event to repository before publishing")
    void shouldSaveEventBeforePublishing() {
        TestEvent event = new TestEvent();
        when(origin.publish(any())).thenReturn(CompletableFuture.completedFuture(
            SendResults.of(List.of(SendResult.success("dest")))));

        publisher.publish(event);

        verify(repository).save(any(EventRecord.class));
        verify(origin).publish(event);
    }

    @Test
    @DisplayName("Should update status to PUBLISHED on success")
    void shouldUpdateStatusOnSuccess() {
        TestEvent event = new TestEvent();
        when(origin.publish(any())).thenReturn(CompletableFuture.completedFuture(
            SendResults.of(List.of(SendResult.success("dest")))));

        publisher.publish(event).join();

        ArgumentCaptor<UUID> idCaptor = ArgumentCaptor.forClass(UUID.class);
        verify(repository).updateStatus(idCaptor.capture(), eq(EventStatus.PUBLISHED), any(Instant.class));
        // Verify that the ID was captured (not null)
        assertNotNull(idCaptor.getValue());
    }

    @Test
    @DisplayName("Should update status to FAILED on error")
    void shouldUpdateStatusOnError() {
        TestEvent event = new TestEvent();
        RuntimeException error = new RuntimeException("Connection failed");
        when(origin.publish(any())).thenReturn(CompletableFuture.failedFuture(error));

        assertThrows(RuntimeException.class, () -> publisher.publish(event).join());

        verify(repository).updateStatus(any(UUID.class), eq(EventStatus.FAILED), eq(null));
    }

    @Test
    @DisplayName("Should extract eventId from Envelope")
    void shouldExtractEventIdFromEnvelope() {
        Envelope<TestEvent> envelope = Envelope.of(new TestEvent());
        UUID expectedId = envelope.eventId();
        when(origin.publish(any())).thenReturn(CompletableFuture.completedFuture(
            SendResults.of(List.of(SendResult.success("dest")))));

        publisher.publish(envelope).join();

        ArgumentCaptor<EventRecord> recordCaptor = ArgumentCaptor.forClass(EventRecord.class);
        verify(repository).save(recordCaptor.capture());
        assertEquals(expectedId, recordCaptor.getValue().id());
    }

    @Test
    @DisplayName("Should call repository save with correct payload")
    void shouldCallRepositorySaveWithCorrectPayload() {
        Envelope<TestEvent> envelope = Envelope.of(new TestEvent());
        when(origin.publish(any())).thenReturn(CompletableFuture.completedFuture(
            SendResults.of(List.of(SendResult.success("dest")))));

        publisher.publish(envelope).join();

        ArgumentCaptor<EventRecord> recordCaptor = ArgumentCaptor.forClass(EventRecord.class);
        verify(repository).save(recordCaptor.capture());
        EventRecord saved = recordCaptor.getValue();
        assertNotNull(saved.payload());
        assertEquals("io.github.vovten.eventflow.event.Envelope", saved.payloadType());
    }

    private static class TestEvent implements Event {
        private final UUID eventId = UUID.randomUUID();
        private final Instant occurredAt = Instant.now();

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        public UUID eventId() {
            return eventId;
        }
    }
}
