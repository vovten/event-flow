package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import com.github.vovten.eventflow.transport.OutgoingEventTransportException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
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
 * Tests for {@link BroadcastKafkaOutgoingEventTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("BroadcastKafkaOutgoingEventTransport Tests")
class BroadcastKafkaOutgoingEventTransportTest {

    @Mock
    private KafkaProducer<String, String> mockProducer;

    @Mock
    private Future<RecordMetadata> mockFuture;

    @Mock
    private RecordMetadata mockMetadata;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, String>> recordCaptor;

    @Test
    @DisplayName("Should create broadcast transport")
    void shouldCreateBroadcastTransport() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        BroadcastKafkaOutgoingEventTransport transport = new BroadcastKafkaOutgoingEventTransport(props, "test-topic");

        assertEquals("kafka", transport.name());
    }

    @Test
    @DisplayName("Should send event to all partitions")
    void shouldSendEventToAllPartitions() throws Exception {
        List<PartitionInfo> partitions = createPartitionInfo(3);
        when(mockProducer.partitionsFor("test-topic")).thenReturn(partitions);
        when(mockProducer.send(recordCaptor.capture())).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any())).thenReturn(mockMetadata);
        when(mockMetadata.hasOffset()).thenReturn(true);

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutgoingEventTransport transport = new BroadcastKafkaOutgoingEventTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        transport.send(event);

        verify(mockProducer, times(3)).send(any());
        List<ProducerRecord<String, String>> capturedRecords = recordCaptor.getAllValues();
        assertThat(capturedRecords).hasSize(3);
        assertThat(capturedRecords.get(0).partition()).isEqualTo(0);
        assertThat(capturedRecords.get(1).partition()).isEqualTo(1);
        assertThat(capturedRecords.get(2).partition()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should throw exception when all partitions fail")
    void shouldThrowExceptionWhenAllPartitionsFail() throws Exception {
        List<PartitionInfo> partitions = createPartitionInfo(2);
        when(mockProducer.partitionsFor("test-topic")).thenReturn(partitions);
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any())).thenThrow(new java.util.concurrent.ExecutionException(
                new org.apache.kafka.common.errors.NetworkException("Network error")
        ));

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutgoingEventTransport transport = new BroadcastKafkaOutgoingEventTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(OutgoingEventTransportException.class)
                .hasMessageContaining("Failed to broadcast event")
                .hasMessageContaining("0/2 successful");
    }

    @Test
    @DisplayName("Should log warning when some partitions fail")
    void shouldLogWarningWhenSomePartitionsFail() throws Exception {
        List<PartitionInfo> partitions = createPartitionInfo(3);
        when(mockProducer.partitionsFor("test-topic")).thenReturn(partitions);
        when(mockProducer.send(any())).thenReturn(mockFuture);

        when(mockFuture.get(anyLong(), any()))
                .thenReturn(mockMetadata)
                .thenReturn(mockMetadata)
                .thenThrow(new java.util.concurrent.ExecutionException(
                        new org.apache.kafka.common.errors.NetworkException("Network error")
                ));

        when(mockMetadata.hasOffset()).thenReturn(true);

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutgoingEventTransport transport = new BroadcastKafkaOutgoingEventTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        transport.send(event);

        verify(mockProducer, times(3)).send(any());
    }

    @Test
    @DisplayName("Should throw exception when topic has no partitions")
    void shouldThrowExceptionWhenTopicHasNoPartitions() {
        when(mockProducer.partitionsFor("test-topic")).thenReturn(List.of());

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutgoingEventTransport transport = new BroadcastKafkaOutgoingEventTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(OutgoingEventTransportException.class)
                .hasMessageContaining("has no partitions");
    }

    @Test
    @DisplayName("Should send to specific partition with correct key and value")
    void shouldSendToSpecificPartitionWithCorrectKeyValue() throws Exception {
        List<PartitionInfo> partitions = createPartitionInfo(1);
        when(mockProducer.partitionsFor("test-topic")).thenReturn(partitions);
        when(mockProducer.send(recordCaptor.capture())).thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any())).thenReturn(mockMetadata);
        when(mockMetadata.hasOffset()).thenReturn(true);

        TestEvent event = TestEvent.create("test-id", "test-message");

        BroadcastKafkaOutgoingEventTransport transport = new BroadcastKafkaOutgoingEventTransport("localhost:9092", "test-topic");
        transport.producer = mockProducer;

        transport.send(event);

        ProducerRecord<String, String> capturedRecord = recordCaptor.getValue();
        assertThat(capturedRecord.topic()).isEqualTo("test-topic");
        assertThat(capturedRecord.key()).isEqualTo(TestEvent.class.getName());
        assertThat(capturedRecord.value()).contains("test-message");
    }

    private List<PartitionInfo> createPartitionInfo(int count) {
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(new PartitionInfo("test-topic", i, null, null, null));
        }
        return partitions;
    }

    private static class BroadcastTestEvent implements Event {
        private final String data;

        BroadcastTestEvent(String data) {
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
