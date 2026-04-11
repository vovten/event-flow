package io.github.vovten.eventflow.transport.incoming;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link KafkaInTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-30
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("KafkaInTransport Tests")
class KafkaInTransportTest {

    @Mock
    private Consumer<String, byte[]> mockConsumer;

    @Test
    @DisplayName("Should create transport with default configuration")
    void shouldCreateTransportWithDefaultConfiguration() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        KafkaInTransport transport = new KafkaInTransport(props, "test-topic", "test-group");

        assertEquals("kafka", transport.name());
    }

    @Test
    @DisplayName("Should receive and deserialize event with JSON format")
    void shouldReceiveAndDeserializeEventWithJsonFormat() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TestEvent event = TestEvent.create("test-id", "test-message");
        JsonEventSerializer serializer = new JsonEventSerializer();
        byte[] eventData = serializer.serialize(event);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), eventData
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(record))
        );

        // Return records once, then return empty records
        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        TestEventConsumer consumer = new TestEventConsumer();

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);

        await().atMost(java.time.Duration.ofSeconds(5)).until(() -> !consumer.getEvents().isEmpty());

        transport.stop();
        executorService.shutdownNow();

        assertEquals(1, consumer.getEvents().size());
        Event receivedEvent = consumer.getEvents().getFirst();
        assertEquals(TestEvent.class, receivedEvent.type());
    }

    @Test
    @DisplayName("Should receive and deserialize event with MessagePack format")
    void shouldReceiveAndDeserializeEventWithMsgPackFormat() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TestEvent event = TestEvent.create("test-id", "test-message");
        MsgPackEventSerializer serializer = new MsgPackEventSerializer();
        byte[] eventData = serializer.serialize(event);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), eventData
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(record))
        );

        // Return records once, then return empty records
        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        TestEventConsumer consumer = new TestEventConsumer();

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);

        await().atMost(java.time.Duration.ofSeconds(5)).until(() -> !consumer.getEvents().isEmpty());

        transport.stop();
        executorService.shutdownNow();

        assertEquals(1, consumer.getEvents().size());
        Event receivedEvent = consumer.getEvents().getFirst();
        assertEquals(TestEvent.class, receivedEvent.type());
    }

    @Test
    @DisplayName("Should handle multiple events")
    void shouldHandleMultipleEvents() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        TestEvent event1 = TestEvent.create("id-1", "message-1");
        TestEvent event2 = TestEvent.create("id-2", "message-2");
        JsonEventSerializer serializer = new JsonEventSerializer();

        ConsumerRecord<String, byte[]> record1 = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), serializer.serialize(event1)
        );
        ConsumerRecord<String, byte[]> record2 = new ConsumerRecord<>(
                "test-topic", 0, 1, TestEvent.class.getName(), serializer.serialize(event2)
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(record1, record2))
        );

        // Return records once, then return empty records
        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        TestEventConsumer consumer = new TestEventConsumer();

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);

        await().atMost(java.time.Duration.ofSeconds(5))
                .until(() -> consumer.getEvents().size() >= 2);

        transport.stop();
        executorService.shutdownNow();

        assertEquals(2, consumer.getEvents().size());
    }

    @Test
    @DisplayName("Should handle old JSON format (backward compatibility)")
    void shouldHandleOldJsonFormat() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        // Old format: JSON string as bytes without magic byte
        String oldJson = "{\"@class\":\"io.github.vovten.eventflow.transport.incoming.KafkaInTransportTest$TestEvent\",\"id\":\"test-id\",\"message\":\"test-message\"}";
        byte[] oldFormatData = oldJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), oldFormatData
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(record))
        );

        // Return records once, then return empty records
        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        TestEventConsumer consumer = new TestEventConsumer();

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);

        await().atMost(java.time.Duration.ofSeconds(5))
                .until(() -> !consumer.getEvents().isEmpty());

        transport.stop();
        executorService.shutdownNow();

        assertEquals(1, consumer.getEvents().size());
    }

    @Test
    @DisplayName("Should handle malformed event gracefully")
    void shouldHandleMalformedEventGracefully() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        // Malformed JSON
        byte[] malformedData = "{invalid json}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        ConsumerRecord<String, byte[]> malformedRecord = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), malformedData
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(malformedRecord))
        );

        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

        TestEventConsumer consumer = new TestEventConsumer();

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);

        // Should not throw exception, just log error
        await().atMost(java.time.Duration.ofSeconds(2))
                .until(() -> true);

        transport.stop();
        executorService.shutdownNow();

        // Malformed event should be skipped
        assertEquals(0, consumer.getEvents().size());
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

    static class TestEventConsumer implements java.util.function.Consumer<Event> {
        private final java.util.List<Event> events = new java.util.ArrayList<>();

        @Override
        public void accept(Event event) {
            events.add(event);
        }

        public List<Event> getEvents() {
            return events;
        }
    }
}
