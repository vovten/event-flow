package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.test.TestEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
        var mockMetadata = mock(RecordMetadata.class);
        when(mockMetadata.hasOffset()).thenReturn(true);
        when(mockMetadata.offset()).thenReturn(0L);
        when(mockProducer.send(any())).thenReturn(CompletableFuture.completedFuture(mockMetadata));
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
            transport.send(event);
        }
        
        // Verify producer was closed via try-with-resources
        verify(mockProducer).close();
    }

    private KafkaOutTransport createTransportWithMockProducer() throws Exception {
        KafkaOutTransport transport = new KafkaOutTransport("localhost:9092", "test-topic");
        Field producerField = KafkaOutTransport.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(transport, mockProducer);
        return transport;
    }
}
