package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.test.TestEvent;
import com.github.vovten.eventflow.transport.SendResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for KafkaOutTransport resource management (AutoCloseable)
 */
@DisplayName("KafkaOutTransport Resource Management Tests")
class KafkaOutTransportResourceTest {

    @Mock
    private KafkaProducer<String, byte[]> mockProducer;

    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
        // Mock async send with immediate success callback
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(mockMetadata(), null);
            return null;
        }).when(mockProducer).send(any(), any(Callback.class));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    @DisplayName("Should implement AutoCloseable")
    void shouldImplementAutoCloseable() throws Exception {
        KafkaOutTransport transport = createTransportWithMockProducer();
        assertInstanceOf(AutoCloseable.class, transport);
    }

    @Test
    @DisplayName("Should close producer when close() is called")
    void shouldCloseProducerOnClose() throws Exception {
        KafkaOutTransport transport = createTransportWithMockProducer();

        transport.close();

        verify(mockProducer).close();
    }

    @Test
    @DisplayName("Should work with try-with-resources")
    void shouldWorkWithTryWithResources() throws Exception {
        TestEvent event = new TestEvent("test-1", "Test message");

        try (KafkaOutTransport transport = createTransportWithMockProducer()) {
            CompletableFuture<SendResult> future = transport.send(event);
            SendResult result = future.join();
            assertThat(result.success()).isTrue();
        }

        // Verify producer was closed via try-with-resources
        verify(mockProducer).close();
    }

    @Test
    @DisplayName("Should throw exception when sending after close")
    void shouldThrowWhenSendingAfterClose() throws Exception {
        TestEvent event = new TestEvent("test-1", "Test message");

        KafkaOutTransport transport = createTransportWithMockProducer();
        transport.close();

        assertThatThrownBy(() -> transport.send(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("KafkaOutTransport is already closed");
    }

    private KafkaOutTransport createTransportWithMockProducer() throws Exception {
        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic");
        Field producerField = KafkaOutTransport.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(transport, mockProducer);
        return transport;
    }

    private org.apache.kafka.clients.producer.RecordMetadata mockMetadata() {
        var metadata = mock(org.apache.kafka.clients.producer.RecordMetadata.class);
        when(metadata.hasOffset()).thenReturn(true);
        when(metadata.offset()).thenReturn(0L);
        when(metadata.partition()).thenReturn(0);
        when(metadata.topic()).thenReturn("test-topic");
        return metadata;
    }
}
