package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import io.github.vovten.eventflow.transport.SendResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.NetworkException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

/**
 * Tests for {@link KafkaOutTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("KafkaOutTransport Tests")
class KafkaOutTransportTest {

    @Mock
    private KafkaProducer<String, byte[]> mockProducer;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor;

    @Captor
    private ArgumentCaptor<Callback> callbackCaptor;

    @Test
    @DisplayName("Should create transport with default JSON serializer")
    void shouldCreateTransportWithDefaultJsonSerializer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        KafkaOutTransport transport = new KafkaOutTransport(props, "test-topic");

        assertEquals("kafka", transport.name());
    }

    @Test
    @DisplayName("Should create transport with custom JSON serializer")
    void shouldCreateTransportWithCustomJsonSerializer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        EventSerializer serializer = new JsonEventSerializer();

        KafkaOutTransport transport = new KafkaOutTransport(props, "test-topic", serializer);

        assertEquals("kafka", transport.name());
    }

    @Test
    @DisplayName("Should create transport with MessagePack serializer")
    void shouldCreateTransportWithMsgPackSerializer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        EventSerializer serializer = new MsgPackEventSerializer();

        KafkaOutTransport transport = new KafkaOutTransport(props, "test-topic", serializer);

        assertEquals("kafka", transport.name());
    }

    @Test
    @DisplayName("Should send event asynchronously with JSON serializer")
    void shouldSendEventAsyncWithJsonSerializer() {
        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;

        // Mock async send with immediate success callback
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(mockMetadata("test-topic", 0, 100), null);
            return null;
        }).when(mockProducer).send(recordCaptor.capture(), callbackCaptor.capture());

        CompletableFuture<SendResult> future = transport.send(event);
        SendResult result = future.join();

        assertThat(result.success()).isTrue();
        assertThat(result.destination()).isEqualTo("test-topic-p0");
        assertThat(result.metadata()).containsEntry("partition", 0);
        assertThat(result.metadata()).containsEntry("offset", 100L);

        ProducerRecord<String, byte[]> capturedRecord = recordCaptor.getValue();
        assertThat(capturedRecord.topic()).isEqualTo("test-topic");
        assertThat(capturedRecord.key()).isEqualTo(TestEvent.class.getName());
        assertThat(capturedRecord.value()).isInstanceOf(byte[].class);
        assertThat(capturedRecord.value()).isNotEmpty();
        assertThat(capturedRecord.value()[0]).isEqualTo((byte) 0x01); // JSON magic byte
    }

    @Test
    @DisplayName("Should send event asynchronously with MessagePack serializer")
    void shouldSendEventAsyncWithMsgPackSerializer() {
        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new MsgPackEventSerializer());
        transport.producer = mockProducer;

        // Mock async send with immediate success callback
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(mockMetadata("test-topic", 1, 200), null);
            return null;
        }).when(mockProducer).send(recordCaptor.capture(), callbackCaptor.capture());

        CompletableFuture<SendResult> future = transport.send(event);
        SendResult result = future.join();

        assertThat(result.success()).isTrue();
        assertThat(result.destination()).isEqualTo("test-topic-p1");
        assertThat(result.metadata()).containsEntry("partition", 1);

        ProducerRecord<String, byte[]> capturedRecord = recordCaptor.getValue();
        assertThat(capturedRecord.topic()).isEqualTo("test-topic");
        assertThat(capturedRecord.key()).isEqualTo(TestEvent.class.getName());
        assertThat(capturedRecord.value()).isInstanceOf(byte[].class);
        assertThat(capturedRecord.value()).isNotEmpty();
        assertThat(capturedRecord.value()[0]).isEqualTo((byte) 0x02); // MessagePack magic byte
    }

    @Test
    @DisplayName("Should handle send failure")
    void shouldHandleSendFailure() {
        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;

        // Mock async send with error callback
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(mockMetadata("test-topic", 0, 0), new NetworkException("Network error"));
            return null;
        }).when(mockProducer).send(any(), any(Callback.class));

        CompletableFuture<SendResult> future = transport.send(event);
        SendResult result = future.join();

        assertThat(result.success()).isFalse();
        assertThat(result.destination()).isEqualTo("test-topic-p0");
        assertThat(result.error()).isInstanceOf(NetworkException.class);
        assertThat(result.errorDetails()).isEqualTo("Network error");
    }

    @Test
    @DisplayName("Should complete exceptionally when serialization fails")
    void shouldCompleteExceptionallyWhenSerializationFails() {
        EventSerializer failingSerializer = new FailingEventSerializer();

        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", failingSerializer);
        transport.producer = mockProducer;

        CompletableFuture<SendResult> future = transport.send(event);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Serialization error");

        verify(mockProducer, never()).send(any(), any());
    }

    @Test
    @DisplayName("Should throw exception when sending on closed transport")
    void shouldThrowExceptionWhenSendingOnClosedTransport() {
        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;
        transport.close();

        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("KafkaOutTransport is already closed");
    }

    @Test
    @DisplayName("Should close producer when close() is called")
    void shouldCloseProducer() {
        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;

        transport.close();

        verify(mockProducer).close();
    }

    @Test
    @DisplayName("Should be idempotent on close")
    void shouldBeIdempotentOnClose() {
        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;

        transport.close();
        transport.close(); // Second close should be safe

        verify(mockProducer, times(1)).close();
    }

    private org.apache.kafka.clients.producer.RecordMetadata mockMetadata(String topic, int partition, long offset) {
        var metadata = mock(org.apache.kafka.clients.producer.RecordMetadata.class);
        lenient().when(metadata.hasOffset()).thenReturn(true);
        lenient().when(metadata.partition()).thenReturn(partition);
        lenient().when(metadata.offset()).thenReturn(offset);
        lenient().when(metadata.topic()).thenReturn(topic);
        return metadata;
    }

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

    static class FailingEventSerializer implements EventSerializer {
        @Override
        public byte[] serialize(Event event) {
            throw new RuntimeException("Serialization error");
        }

        @Override
        public <T extends Event> T deserialize(byte[] data, Class<T> eventType) {
            throw new RuntimeException("Deserialization error");
        }

        @Override
        public byte getCode() {
            return (byte) 0x99;
        }

        @Override
        public String getName() {
            return "failing";
        }
    }
}
