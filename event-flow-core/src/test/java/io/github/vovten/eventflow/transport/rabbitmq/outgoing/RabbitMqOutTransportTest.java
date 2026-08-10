package io.github.vovten.eventflow.transport.rabbitmq.outgoing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionHolder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RabbitMqOutTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RabbitMqOutTransport Tests")
class RabbitMqOutTransportTest {

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    @Test
    @DisplayName("Should publish serialized event to the default exchange")
    void shouldPublishSerializedEventToDefaultExchange() throws IOException {
        // given
        when(connection.createChannel()).thenReturn(channel);
        RabbitMqOutTransport transport = createTransport("", "test-queue");
        TestEvent event = TestEvent.create("id-1", "message-1");

        // when
        CompletableFuture<SendResult> future = transport.send(event);
        SendResult result = future.join();

        // then
        assertThat(result.success()).isTrue();
        assertThat(result.destination()).isEqualTo("rabbitmq-default/test-queue");
        ArgumentCaptor<byte[]> bodyCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(channel).basicPublish(eq(""), eq("test-queue"), isNull(), bodyCaptor.capture());
        assertThat(bodyCaptor.getValue()).isNotEmpty();
        assertThat(bodyCaptor.getValue()[0]).isEqualTo((byte) 0x01);

        transport.close();
    }

    @Test
    @DisplayName("Should publish event serialized with MessagePack format")
    void shouldPublishEventSerializedWithMsgPackFormat() throws IOException {
        // given
        when(connection.createChannel()).thenReturn(channel);
        RabbitMqOutTransport transport = new RabbitMqOutTransport(connection, "events", "orders",
                new MsgPackEventSerializer(), new RabbitMqConnectionHolder(connection));
        TestEvent event = TestEvent.create("id-2", "message-2");

        // when
        SendResult result = transport.send(event).join();

        // then
        assertThat(result.success()).isTrue();
        assertThat(result.destination()).isEqualTo("rabbitmq-events/orders");
        ArgumentCaptor<byte[]> bodyCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(channel).basicPublish(eq("events"), eq("orders"), isNull(), bodyCaptor.capture());
        assertThat(bodyCaptor.getValue()[0]).isEqualTo((byte) 0x02);

        transport.close();
    }

    @Test
    @DisplayName("Should throw IllegalStateException when sending on a closed transport")
    void shouldThrowWhenSendingOnClosedTransport() throws Exception {
        // given
        when(connection.createChannel()).thenReturn(channel);
        RabbitMqOutTransport transport = createTransport("", "test-queue");
        transport.close();
        TestEvent event = TestEvent.create("id-3", "message-3");

        // when & then
        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("RabbitMqOutTransport is already closed");
    }

    @Test
    @DisplayName("Should return failure result when the broker rejects the publish")
    void shouldReturnFailureWhenBrokerRejectsPublish() throws IOException {
        // given
        when(connection.createChannel()).thenReturn(channel);
        RabbitMqOutTransport transport = createTransport("", "test-queue");
        doThrow(new IOException("Broker unreachable"))
                .when(channel).basicPublish(anyString(), anyString(), isNull(), any(byte[].class));
        TestEvent event = TestEvent.create("id-4", "message-4");

        // when
        SendResult result = transport.send(event).join();

        // then
        assertThat(result.success()).isFalse();
        assertThat(result.error()).isNotNull();
        assertThat(result.errorDetails()).contains("Broker unreachable");

        transport.close();
    }

    @Test
    @DisplayName("Should close channel and release connection on close")
    void shouldCloseChannelAndReleaseConnection() throws Exception {
        // given
        when(connection.createChannel()).thenReturn(channel);
        RabbitMqOutTransport transport = createTransport("", "test-queue");

        // when
        transport.close();

        // then
        verify(channel).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("Should be idempotent on close")
    void shouldBeIdempotentOnClose() throws Exception {
        // given
        when(connection.createChannel()).thenReturn(channel);
        RabbitMqOutTransport transport = createTransport("", "test-queue");

        // when
        transport.close();
        transport.close();

        // then
        verify(channel, times(1)).close();
        verify(connection, times(1)).close();
    }

    @Test
    @DisplayName("Should have the correct transport name")
    void shouldHaveCorrectTransportName() throws Exception {
        // given
        when(connection.createChannel()).thenReturn(channel);
        RabbitMqOutTransport transport = createTransport("", "test-queue");

        // when & then
        assertThat(transport.name()).isEqualTo("rabbitmq");

        transport.close();
    }

    private RabbitMqOutTransport createTransport(String exchange, String routingKey) {
        return new RabbitMqOutTransport(connection, exchange, routingKey,
                new JsonEventSerializer(), new RabbitMqConnectionHolder(connection));
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
