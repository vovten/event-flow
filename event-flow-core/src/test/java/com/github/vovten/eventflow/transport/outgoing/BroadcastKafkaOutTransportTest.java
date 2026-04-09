package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.test.TestEvent;
import com.github.vovten.eventflow.transport.SendResult;
import com.github.vovten.eventflow.transport.TransportException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.NetworkException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link BroadcastKafkaOutTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("BroadcastKafkaPublisherTransport Tests")
class BroadcastKafkaOutTransportTest {

    @Mock
    private KafkaProducer<String, byte[]> mockProducer;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor;

    @Test
    @DisplayName("Should create broadcast transport")
    void shouldCreateBroadcastTransport() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        BroadcastKafkaOutTransport transport = new BroadcastKafkaOutTransport(props, "test-topic");

        assertEquals("kafka", transport.name());
    }

    @Test
    @DisplayName("Should send event to all partitions asynchronously")
    void shouldSendEventToAllPartitions() {
        List<PartitionInfo> partitions = createPartitionInfo(3);
        when(mockProducer.partitionsFor("test-topic")).thenReturn(partitions);

        // Mock async send with callback
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(mockMetadata(), null);
            return null;
        }).when(mockProducer).send(recordCaptor.capture(), any(Callback.class));

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutTransport transport = new BroadcastKafkaOutTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        CompletableFuture<SendResult> future = transport.send(event);
        SendResult result = future.join();

        assertThat(result.success()).isTrue();
        verify(mockProducer, times(3)).send(any(), any(Callback.class));
        List<ProducerRecord<String, byte[]>> capturedRecords = recordCaptor.getAllValues();
        assertThat(capturedRecords).hasSize(3);
        assertThat(capturedRecords.get(0).partition()).isEqualTo(0);
        assertThat(capturedRecords.get(1).partition()).isEqualTo(1);
        assertThat(capturedRecords.get(2).partition()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should return failed result when all partitions fail")
    void shouldReturnFailedResultWhenAllPartitionsFail() {
        List<PartitionInfo> partitions = createPartitionInfo(2);
        when(mockProducer.partitionsFor("test-topic")).thenReturn(partitions);

        // Mock async send with callback that reports error
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, new NetworkException("Network error"));
            return null;
        }).when(mockProducer).send(any(), any(Callback.class));

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutTransport transport = new BroadcastKafkaOutTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        CompletableFuture<SendResult> future = transport.send(event);
        SendResult result = future.join();

        assertThat(result.success()).isFalse();
        assertThat(result.error()).isInstanceOf(NetworkException.class);
        assertThat(result.errorDetails()).contains("Failed to broadcast event");
        assertThat(result.errorDetails()).contains("0/2 successful");
    }

    @Test
    @DisplayName("Should succeed with partial failures and log warning")
    void shouldSucceedWithPartialFailures() {
        List<PartitionInfo> partitions = createPartitionInfo(3);
        when(mockProducer.partitionsFor("test-topic")).thenReturn(partitions);

        // Mock async send - first 2 succeed, 3rd fails
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(mockMetadata(), null);
            return null;
        }).doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(mockMetadata(), null);
            return null;
        }).doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, new NetworkException("Network error"));
            return null;
        }).when(mockProducer).send(any(), any(Callback.class));

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutTransport transport = new BroadcastKafkaOutTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        CompletableFuture<SendResult> future = transport.send(event);
        SendResult result = future.join();

        assertThat(result.success()).isTrue();
        verify(mockProducer, times(3)).send(any(), any(Callback.class));
    }

    @Test
    @DisplayName("Should throw exception when topic has no partitions")
    void shouldThrowExceptionWhenTopicHasNoPartitions() {
        when(mockProducer.partitionsFor("test-topic")).thenReturn(List.of());

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutTransport transport = new BroadcastKafkaOutTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("has no partitions");
    }

    @Test
    @DisplayName("Should send to specific partition with correct key and value")
    void shouldSendToSpecificPartitionWithCorrectKeyValue() {
        List<PartitionInfo> partitions = createPartitionInfo(1);
        when(mockProducer.partitionsFor("test-topic")).thenReturn(partitions);

        // Mock async send with callback
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(mockMetadata(), null);
            return null;
        }).when(mockProducer).send(recordCaptor.capture(), any(Callback.class));

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutTransport transport = new BroadcastKafkaOutTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        CompletableFuture<SendResult> future = transport.send(event);
        future.join();

        ProducerRecord<String, byte[]> capturedRecord = recordCaptor.getValue();
        assertThat(capturedRecord.topic()).isEqualTo("test-topic");
        assertThat(capturedRecord.key()).isEqualTo(TestEvent.class.getName());
        assertThat(capturedRecord.value()).isNotNull();
    }

    private List<PartitionInfo> createPartitionInfo(int count) {
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(new PartitionInfo("test-topic", i, null, null, null));
        }
        return partitions;
    }

    private org.apache.kafka.clients.producer.RecordMetadata mockMetadata() {
        var metadata = mock(org.apache.kafka.clients.producer.RecordMetadata.class);
        lenient().when(metadata.hasOffset()).thenReturn(true);
        lenient().when(metadata.partition()).thenReturn(0);
        lenient().when(metadata.offset()).thenReturn(100L);
        lenient().when(metadata.topic()).thenReturn("test-topic");
        return metadata;
    }

    private static class BroadcastTestEvent extends AbstractTraceableEvent {
        private final String data;

        BroadcastTestEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return BroadcastTestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\",\"timestamp\":\"" + LocalDateTime.now() + "\"}";
        }
    }
}
