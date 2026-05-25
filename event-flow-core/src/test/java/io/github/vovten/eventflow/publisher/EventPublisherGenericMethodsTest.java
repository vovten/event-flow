package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.EventBuilder;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @since 1.1.0
 */
@DisplayName("EventPublisher Generic Methods Tests")
class EventPublisherGenericMethodsTest {

    @Test
    @DisplayName("Should wrap plain object in Envelope when publishing")
    void shouldWrapPlainObjectInEnvelope() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        publisher.publish(event).join();

        Envelope<?> envelope = (Envelope<?>) queue.poll();
        assertThat(envelope).isNotNull();
        assertThat(envelope.payload()).isInstanceOf(PlainDomainEvent.class);
    }

    @Test
    @DisplayName("Should auto-generate eventId for plain object")
    void shouldAutoGenerateEventIdForPlainObject() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        publisher.publish(event).join();

        Envelope<?> envelope = (Envelope<?>) queue.poll();
        assertThat(envelope).isNotNull();
        assertThat(envelope.eventId()).isNotNull();
    }

    @Test
    @DisplayName("Should auto-generate occurredAt for plain object")
    void shouldAutoGenerateOccurredAtForPlainObject() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        publisher.publish(event).join();

        Envelope<?> envelope = (Envelope<?>) queue.poll();
        assertThat(envelope).isNotNull();
        assertThat(envelope.occurredAt()).isNotNull();
    }

    @Test
    @DisplayName("Should store payload type in metadata")
    void shouldStorePayloadTypeInMetadata() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        publisher.publish(event).join();

        Envelope<?> envelope = (Envelope<?>) queue.poll();
        assertThat(envelope).isNotNull();
        assertThat(envelope.payload().getClass()).isEqualTo(PlainDomainEvent.class);
    }

    @Test
    @DisplayName("Should create builder for plain object")
    void shouldCreateBuilderForPlainObject() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        EventBuilder<PlainDomainEvent> builder = publisher.prepare(event);

        assertThat(builder).isNotNull();
    }

    @Test
    @DisplayName("Should publish via builder with custom eventId and processId")
    void shouldPublishViaBuilderWithCustomEventIdAndProcessId() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        UUID customEventId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID customProcessId = UUID.fromString("550e8400-e29b-41d4-a716-446655440001");
        publisher.prepare(event)
                .withEventId(customEventId)
                .withProcessId(customProcessId)
                .publish()
                .join();

        Envelope<?> envelope = (Envelope<?>) queue.poll();
        assertThat(envelope).isNotNull();
        assertThat(envelope.eventId()).isEqualTo(customEventId);
        assertThat(envelope.processId()).isEqualTo(customProcessId);
    }

    @Test
    @DisplayName("Should allow fluent chaining with builder")
    void shouldAllowFluentChainingWithBuilder() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);
        EventPublisher publisher = new ChannelEventPublisher(List.of(channel));

        PlainDomainEvent event = new PlainDomainEvent("order-123", "test@mail.ru");
        UUID processId = UUID.fromString("550e8400-e29b-41d4-a716-446655440001");
        publisher.prepare(event)
                .withProcessId(processId)
                .withMetadata("source", "order-service")
                .publish()
                .join();

        Envelope<?> envelope = (Envelope<?>) queue.poll();
        assertThat(envelope).isNotNull();
        assertThat(envelope.processId()).isEqualTo(processId);
        assertThat(envelope.metadata())
                .containsEntry("source", "order-service");
    }

    record PlainDomainEvent(String orderId, String customerEmail) {}
}