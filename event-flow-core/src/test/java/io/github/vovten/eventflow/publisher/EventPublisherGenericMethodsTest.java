package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.EventBuilder;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("EventPublisher Generic Methods Tests")
class EventPublisherGenericMethodsTest {

    @Test
    @DisplayName("Should wrap plain object in Envelope when publishing")
    void shouldWrapPlainObjectInEnvelope() {
        OutTransport transport = mock(OutTransport.class);
        when(transport.send(any())).thenAnswer(invocation -> {
            Event argument = invocation.getArgument(0);
            assertThat(argument).isInstanceOf(Envelope.class);
            Envelope<?> envelope = (Envelope<?>) argument;
            assertThat(envelope.payload()).isInstanceOf(PlainDomainEvent.class);
            return CompletableFuture.completedFuture(SendResult.success("dest"));
        });
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        publisher.publish(event).join();

        verify(transport).send(any(Envelope.class));
    }

    @Test
    @DisplayName("Should auto-generate eventId for plain object")
    void shouldAutoGenerateEventIdForPlainObject() {
        OutTransport transport = mock(OutTransport.class);
        when(transport.send(any())).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest")));
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        publisher.publish(event).join();

        verify(transport).send(argThat((Envelope<?> e) -> e.eventId() != null));
    }

    @Test
    @DisplayName("Should auto-generate occurredAt for plain object")
    void shouldAutoGenerateOccurredAtForPlainObject() {
        OutTransport transport = mock(OutTransport.class);
        when(transport.send(any())).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest")));
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        publisher.publish(event).join();

        verify(transport).send(argThat((Envelope<?> e) -> e.occurredAt() != null));
    }

    @Test
    @DisplayName("Should store payload type in metadata")
    void shouldStorePayloadTypeInMetadata() {
        OutTransport transport = mock(OutTransport.class);
        when(transport.send(any())).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest")));
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        publisher.publish(event).join();

        verify(transport).send(argThat((Envelope<?> e) ->
                e.getPayloadType().equals(PlainDomainEvent.class.getName())));
    }

    @Test
    @DisplayName("Should create builder for plain object")
    void shouldCreateBuilderForPlainObject() {
        OutTransport transport = mock(OutTransport.class);
        when(transport.send(any())).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest")));
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        EventBuilder<PlainDomainEvent> builder = publisher.prepare(event);

        assertThat(builder).isNotNull();
    }

    @Test
    @DisplayName("Should publish via builder with custom traceId")
    void shouldPublishViaBuilderWithCustomTraceId() {
        OutTransport transport = mock(OutTransport.class);
        when(transport.send(any())).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest")));
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        UUID customTraceId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        publisher.prepare(event)
                .withTraceId(customTraceId)
                .publish()
                .join();

        verify(transport).send(argThat((Envelope<?> e) ->
                e.traceId().equals(customTraceId)));
    }

    @Test
    @DisplayName("Should allow fluent chaining with builder")
    void shouldAllowFluentChainingWithBuilder() {
        OutTransport transport = mock(OutTransport.class);
        when(transport.send(any())).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest")));
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        UUID traceId = UUID.fromString("550e8400-e29b-41d4-a716-446655440001");
        publisher.prepare(event)
                .withTraceId(traceId)
                .withMetadata("source", "order-service")
                .publish()
                .join();

        verify(transport).send(argThat((Envelope<?> e) ->
                e.traceId().equals(traceId) &&
                        e.metadata().get("source").equals("order-service")));
    }

    record PlainDomainEvent(String orderId, String customerEmail) {}
}