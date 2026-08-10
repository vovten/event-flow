package io.github.vovten.eventflow.transport.rabbitmq.incoming;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionHolder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RabbitMqInTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RabbitMqInTransport Tests")
class RabbitMqInTransportTest {

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    @Test
    @DisplayName("Should declare the configured queue on start")
    void shouldDeclareConfiguredQueueOnStart() throws IOException {
        // given
        RabbitMqInTransport transport = createTransport("test-queue", false);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class)))
                .thenReturn("consumer-tag-1");

        // when
        transport.start(event -> { });

        // then
        verify(channel).queueDeclare("test-queue", false, false, false, null);

        transport.stop();
    }

    @Test
    @DisplayName("Should deliver deserialized events from the queue to the consumer")
    void shouldDeliverDeserializedEventsToConsumer() throws IOException {
        // given
        RabbitMqInTransport transport = createTransport("test-queue", false);
        when(connection.createChannel()).thenReturn(channel);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        when(channel.basicConsume(eq("test-queue"), eq(true), callbackCaptor.capture(), any(CancelCallback.class)))
                .thenReturn("consumer-tag-1");

        TestEvent event = TestEvent.create("id-1", "message-1");
        byte[] body = new JsonEventSerializer().serialize(event);
        List<Event> received = new ArrayList<>();

        // when
        transport.start(received::add);
        callbackCaptor.getValue().handle("consumer-tag-1",
                new Delivery(new Envelope(1, false, "", "test-queue"), new AMQP.BasicProperties(), body));

        // then
        assertThat(received).hasSize(1);
        assertThat(received.get(0)).isInstanceOf(TestEvent.class);
        assertThat(received.get(0).type()).isEqualTo(TestEvent.class);

        transport.stop();
    }

    @Test
    @DisplayName("Should deliver events serialized with MessagePack format")
    void shouldDeliverEventsSerializedWithMsgPackFormat() throws IOException {
        // given
        RabbitMqInTransport transport = createTransport("test-queue", false);
        when(connection.createChannel()).thenReturn(channel);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        when(channel.basicConsume(eq("test-queue"), eq(true), callbackCaptor.capture(), any(CancelCallback.class)))
                .thenReturn("consumer-tag-1");

        TestEvent event = TestEvent.create("id-2", "message-2");
        byte[] body = new io.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer().serialize(event);
        List<Event> received = new ArrayList<>();

        // when
        transport.start(received::add);
        callbackCaptor.getValue().handle("consumer-tag-1",
                new Delivery(new Envelope(1, false, "", "test-queue"), new AMQP.BasicProperties(), body));

        // then
        assertThat(received).hasSize(1);
        assertThat(received.get(0).type()).isEqualTo(TestEvent.class);

        transport.stop();
    }

    @Test
    @DisplayName("Should skip malformed messages and keep consuming")
    void shouldSkipMalformedMessagesAndKeepConsuming() throws IOException {
        // given
        RabbitMqInTransport transport = createTransport("test-queue", false);
        when(connection.createChannel()).thenReturn(channel);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        when(channel.basicConsume(eq("test-queue"), eq(true), callbackCaptor.capture(), any(CancelCallback.class)))
                .thenReturn("consumer-tag-1");

        TestEvent event = TestEvent.create("id-3", "message-3");
        byte[] validBody = new JsonEventSerializer().serialize(event);
        byte[] malformedBody = new byte[]{(byte) 0x55, 1, 2, 3};
        List<Event> received = new ArrayList<>();

        // when
        transport.start(received::add);
        DeliverCallback callback = callbackCaptor.getValue();
        callback.handle("consumer-tag-1",
                new Delivery(new Envelope(1, false, "", "test-queue"), new AMQP.BasicProperties(), malformedBody));
        callback.handle("consumer-tag-1",
                new Delivery(new Envelope(2, false, "", "test-queue"), new AMQP.BasicProperties(), validBody));

        // then
        assertThat(received).hasSize(1);
        assertThat(received.get(0).type()).isEqualTo(TestEvent.class);

        transport.stop();
    }

    @Test
    @DisplayName("Should be idempotent on start")
    void shouldBeIdempotentOnStart() throws IOException {
        // given
        RabbitMqInTransport transport = createTransport("test-queue", false);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class)))
                .thenReturn("consumer-tag-1");

        // when
        transport.start(event -> { });
        transport.start(event -> { });

        // then
        verify(connection, times(1)).createChannel();
        verify(channel, times(1)).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any());
        verify(channel, times(1)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));

        transport.stop();
    }

    @Test
    @DisplayName("Should cancel consumer, close channel and release connection on stop")
    void shouldCancelConsumerAndReleaseResourcesOnStop() throws Exception {
        // given
        RabbitMqInTransport transport = createTransport("test-queue", false);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class)))
                .thenReturn("consumer-tag-1");
        transport.start(event -> { });

        // when
        transport.stop();

        // then
        verify(channel).basicCancel("consumer-tag-1");
        verify(channel).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("Should not cancel consumer or close channel when stop called before start")
    void shouldNotCancelConsumerWhenStopBeforeStart() throws IOException {
        // given
        RabbitMqInTransport transport = createTransport("test-queue", false);

        // when
        transport.stop();

        // then
        verify(connection, never()).createChannel();
        verify(connection, never()).close();
    }

    private RabbitMqInTransport createTransport(String queue, boolean queueDurable) {
        RabbitMqConnectionHolder holder = new RabbitMqConnectionHolder(connection);
        return new RabbitMqInTransport(connection, queue, queueDurable, new EventSerializerFactory(), holder);
    }

    /**
     * Test event class.
     */
    static class TestEvent extends AbstractTraceableEvent {
        public String id;
        public String message;

        TestEvent() {
            super();
        }

        TestEvent(String id, String message) {
            super();
            this.id = id;
            this.message = message;
        }

        static TestEvent create(String id, String message) {
            return new TestEvent(id, message);
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
