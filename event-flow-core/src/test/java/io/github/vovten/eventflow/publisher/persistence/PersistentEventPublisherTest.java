package io.github.vovten.eventflow.publisher.persistence;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("PersistentEventPublisher Tests")
class PersistentEventPublisherTest {

    @Mock
    private EventRepository repository;

    @Mock
    private EventSerializer serializer;

    private EventPublisher realDelegate;
    private EventPublisher spyDelegate;
    private PersistentEventPublisher publisher;
    private CompletableFuture<SendResults> successFuture;

    @BeforeEach
    void setUp() {
        realDelegate = new TestEventPublisher();
        spyDelegate = spy(realDelegate);
        
        publisher = new PersistentEventPublisher(spyDelegate, repository, serializer);
        successFuture = CompletableFuture.completedFuture(
            SendResults.of(List.of(SendResult.success("dest"))));
        
        when(serializer.serialize(any(Event.class))).thenReturn("{}".getBytes());
    }

    @Test
    @DisplayName("Should save event to repository before publishing")
    void shouldSaveEventBeforePublishing() {
        TestEvent event = new TestEvent();
        doReturn(successFuture).when(spyDelegate).publish(any());

        publisher.publish(event);

        verify(repository).save(any(EventRecord.class));
        verify(spyDelegate).publish(any());
    }

    @Test
    @DisplayName("Should update status to PUBLISHED on success")
    void shouldUpdateStatusOnSuccess() {
        TestEvent event = new TestEvent();
        doReturn(successFuture).when(spyDelegate).publish(any());

        publisher.publish(event).join();

        ArgumentCaptor<UUID> idCaptor = ArgumentCaptor.forClass(UUID.class);
        verify(repository).updateStatus(idCaptor.capture(), eq(EventStatus.PUBLISHED), any(Instant.class));
        assertNotNull(idCaptor.getValue());
    }

    @Test
    @DisplayName("Should update status to FAILED on error with error message")
    void shouldUpdateStatusOnErrorWithErrorMessage() {
        TestEvent event = new TestEvent();
        RuntimeException error = new RuntimeException("Connection failed");
        CompletableFuture<SendResults> failedFuture = CompletableFuture.failedFuture(error);
        doReturn(failedFuture).when(spyDelegate).publish(any());

        assertThrows(RuntimeException.class, () -> publisher.publish(event).join());
        
        ArgumentCaptor<UUID> idCaptor = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<String> msgCaptor = ArgumentCaptor.forClass(String.class);
        verify(repository).updateStatus(idCaptor.capture(), eq(EventStatus.FAILED), msgCaptor.capture());
        assertTrue(msgCaptor.getValue().contains("Connection failed"));
    }

    @Test
    @DisplayName("Should extract eventId from Envelope")
    void shouldExtractEventIdFromEnvelope() {
        Envelope<TestEvent> envelope = Envelope.of(new TestEvent());
        doReturn(successFuture).when(spyDelegate).publish(any());

        publisher.publish(envelope).join();

        ArgumentCaptor<EventRecord> recordCaptor = ArgumentCaptor.forClass(EventRecord.class);
        verify(repository).save(recordCaptor.capture());
        
        // The saved event ID should match the envelope's event ID
        assertEquals(envelope.eventId(), recordCaptor.getValue().id());
    }

    @Test
    @DisplayName("Should serialize event to JSON")
    void shouldSerializeEventToJson() {
        Envelope<TestEvent> envelope = Envelope.of(new TestEvent());
        byte[] expectedBytes = "{\"test\":true}".getBytes();
        doReturn(successFuture).when(spyDelegate).publish(any());
        when(serializer.serialize(any(Event.class))).thenReturn(expectedBytes);

        publisher.publish(envelope).join();

        verify(serializer).serialize(any(Event.class));
        ArgumentCaptor<EventRecord> recordCaptor = ArgumentCaptor.forClass(EventRecord.class);
        verify(repository).save(recordCaptor.capture());
        assertEquals(new String(expectedBytes), recordCaptor.getValue().payload());
    }

    private static class TestEventPublisher implements EventPublisher {
        @Override
        public CompletableFuture<SendResults> publish(Event event) {
            return CompletableFuture.completedFuture(
                SendResults.of(List.of(SendResult.success("test"))));
        }
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