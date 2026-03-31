package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import com.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import com.github.vovten.eventflow.transport.TransportException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Mock
    private Future<RecordMetadata> mockFuture;

    @Mock
    private RecordMetadata mockMetadata;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor;

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
    @DisplayName("Should send event with JSON serializer")
    void shouldSendEventWithJsonSerializer() throws Exception {
        when(mockProducer.send(recordCaptor.capture())).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any())).thenReturn(mockMetadata);
        when(mockMetadata.hasOffset()).thenReturn(true);

        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;

        transport.send(event);

        verify(mockProducer, times(1)).send(any());
        ProducerRecord<String, byte[]> capturedRecord = recordCaptor.getValue();
        assertThat(capturedRecord.topic()).isEqualTo("test-topic");
        assertThat(capturedRecord.key()).isEqualTo(TestEvent.class.getName());
        assertThat(capturedRecord.value()).isInstanceOf(byte[].class);
        assertThat(capturedRecord.value()).isNotEmpty();
        assertThat(capturedRecord.value()[0]).isEqualTo((byte) 0x01); // JSON magic byte
    }

    @Test
    @DisplayName("Should send event with MessagePack serializer")
    void shouldSendEventWithMsgPackSerializer() throws Exception {
        when(mockProducer.send(recordCaptor.capture())).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any())).thenReturn(mockMetadata);
        when(mockMetadata.hasOffset()).thenReturn(true);

        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new MsgPackEventSerializer());
        transport.producer = mockProducer;

        transport.send(event);

        verify(mockProducer, times(1)).send(any());
        ProducerRecord<String, byte[]> capturedRecord = recordCaptor.getValue();
        assertThat(capturedRecord.topic()).isEqualTo("test-topic");
        assertThat(capturedRecord.key()).isEqualTo(TestEvent.class.getName());
        assertThat(capturedRecord.value()).isInstanceOf(byte[].class);
        assertThat(capturedRecord.value()).isNotEmpty();
        assertThat(capturedRecord.value()[0]).isEqualTo((byte) 0x02); // MessagePack magic byte
    }

    @Test
    @DisplayName("Should throw exception when send fails")
    void shouldThrowExceptionWhenSendFails() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any())).thenThrow(
                new java.util.concurrent.ExecutionException(
                        new org.apache.kafka.common.errors.NetworkException("Network error")
                )
        );

        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;

        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("Failed to send event");
    }

    @Test
    @DisplayName("Should throw exception on timeout")
    void shouldThrowExceptionOnTimeout() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any())).thenThrow(
                new java.util.concurrent.TimeoutException("Timeout")
        );

        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;

        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("Timeout");
    }

    @Test
    @DisplayName("Should throw exception on interrupt")
    void shouldThrowExceptionOnInterrupt() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any())).thenThrow(
                new InterruptedException("Interrupted")
        );

        TestEvent event = TestEvent.create("test-id", "test-message");

        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic", new JsonEventSerializer());
        transport.producer = mockProducer;

        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("interrupted");
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
}
