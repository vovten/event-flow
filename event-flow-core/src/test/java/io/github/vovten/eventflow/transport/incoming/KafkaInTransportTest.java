package io.github.vovten.eventflow.transport.incoming;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.serialization.msgpack.MsgPackEventSerializer;
import io.github.vovten.eventflow.transport.AsyncEventConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link KafkaInTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("KafkaInTransport Tests")
class KafkaInTransportTest {

    @Mock
    private Consumer<String, byte[]> mockConsumer;

    /** Records every commitSync batch so tests can assert committed offsets. */
    private final List<Map<TopicPartition, OffsetAndMetadata>> committedBatches =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    @Test
    @DisplayName("Should treat a null processing future as a contract violation and block the partition")
    void shouldBlockPartitionOnNullProcessingFuture() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        JsonEventSerializer serializer = new JsonEventSerializer();
        TopicPartition partition = new TopicPartition("test-topic", 0);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-0", "m0"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(partition, List.of(record))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());
        when(mockConsumer.assignment()).thenReturn(Set.of(partition));

        recordCommits();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        // A null future is a contract violation and must not be treated as success.
        transport.start(e -> null);

        verify(mockConsumer, timeout(5000)).pause(argThat(partitions -> partitions.contains(partition)));
        assertTrue(committedBatches.isEmpty(), "A contract violation must not commit the offset");

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should skip a tombstone record and commit its offset")
    void shouldSkipTombstoneRecord() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TopicPartition partition = new TopicPartition("test-topic", 0);

        ConsumerRecord<String, byte[]> tombstone = new ConsumerRecord<>(
                "test-topic", 0, 0, "key", null
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(partition, List.of(tombstone))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        recordCommits();
        TestEventConsumer consumer = new TestEventConsumer();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);

        await().atMost(Duration.ofSeconds(5)).until(() -> committedOffset(partition) == 1L);
        assertEquals(0, consumer.getEvents().size());

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should block the partition on a serializer configuration error instead of skipping the record")
    void shouldBlockPartitionOnSerializerConfigurationError() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TopicPartition partition = new TopicPartition("test-topic", 0);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), new byte[]{0x01, 0x02}
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(partition, List.of(record))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());
        when(mockConsumer.assignment()).thenReturn(Set.of(partition));

        EventSerializerFactory brokenFactory = mock(EventSerializerFactory.class);
        when(brokenFactory.getByData(any())).thenThrow(new IllegalStateException("serializer misconfigured"));

        recordCommits();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, brokenFactory
        );

        transport.start(e -> CompletableFuture.completedFuture(null));

        verify(mockConsumer, timeout(5000)).pause(argThat(partitions -> partitions.contains(partition)));
        assertTrue(committedBatches.isEmpty(),
                "A configuration error must not silently skip the record");

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should retry a failed commit without losing the completed frontier")
    void shouldRetryCommitWithoutLosingFrontier() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TopicPartition partition = new TopicPartition("test-topic", 0);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(),
                new JsonEventSerializer().serialize(TestEvent.create("id-0", "m0"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(partition, List.of(record))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        // The first commit fails transiently; the same frontier must be retried.
        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        lenient().doAnswer(invocation -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("transient commit failure");
            }
            committedBatches.add(new java.util.HashMap<>(invocation.getArgument(0)));
            return null;
        }).when(mockConsumer).commitSync(anyMap());

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(e -> CompletableFuture.completedFuture(null));

        await().atMost(Duration.ofSeconds(5)).until(() -> committedOffset(partition) == 1L);
        assertTrue(attempts.get() >= 2,
                "A failed commit must be retried instead of losing the completed frontier");

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should reject a restart after the transport has been stopped")
    void shouldRejectRestartAfterStop() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        lenient().when(mockConsumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );
        transport.start(e -> CompletableFuture.completedFuture(null));
        transport.stop();

        assertThrows(IllegalStateException.class,
                () -> transport.start(e -> CompletableFuture.completedFuture(null)),
                "A stopped transport must not be restartable");

        try {
            executorService.shutdownNow();
        } catch (Exception ignored) {
            // already shut down by stop()
        }
    }

    @Test
    @DisplayName("Should stop the loop and shut down the executor after a fatal consumer error")
    void shouldShutDownExecutorAfterFatalConsumerError() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        when(mockConsumer.poll(any(Duration.class)))
                .thenThrow(new AuthenticationException("fatal consumer error"));

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );
        transport.start(e -> CompletableFuture.completedFuture(null));

        await().atMost(Duration.ofSeconds(5)).until(executorService::isTerminated);
        assertDoesNotThrow(transport::stop,
                "Stopping an already terminated transport must not throw");
    }

    @Test
    @DisplayName("Should keep the blocked state when unblock's seek fails and retry on the next request")
    void shouldRetryUnblockWhenSeekFails() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        JsonEventSerializer serializer = new JsonEventSerializer();
        TopicPartition partition = new TopicPartition("test-topic", 0);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-0", "m0"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(partition, List.of(record))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());
        when(mockConsumer.assignment()).thenReturn(Set.of(partition));

        CompletableFuture<Object> failing = new CompletableFuture<>();
        java.util.concurrent.atomic.AtomicInteger delivered = new java.util.concurrent.atomic.AtomicInteger();
        AsyncEventConsumer consumer = event -> {
            delivered.incrementAndGet();
            return failing;
        };

        recordCommits();
        // The first seek fails, the second succeeds. Stub before the loop starts
        // to avoid concurrent Mockito stubbing while the transport is running.
        java.util.concurrent.atomic.AtomicInteger seekAttempts = new java.util.concurrent.atomic.AtomicInteger();
        lenient().doAnswer(invocation -> {
            if (seekAttempts.incrementAndGet() == 1) {
                throw new IllegalStateException("seek failed");
            }
            return null;
        }).when(mockConsumer).seek(eq(partition), eq(0L));

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);
        await().atMost(Duration.ofSeconds(5)).until(() -> delivered.get() == 1);
        failing.completeExceptionally(new IllegalStateException("handler failure"));
        verify(mockConsumer, timeout(5000)).pause(argThat(partitions -> partitions.contains(partition)));

        transport.unblock(partition);
        verify(mockConsumer, timeout(5000)).seek(eq(partition), eq(0L));

        // Second request retries the seek and finally releases the partition.
        transport.unblock(partition);
        verify(mockConsumer, timeout(5000).times(2)).seek(eq(partition), eq(0L));
        verify(mockConsumer, timeout(5000)).resume(argThat(partitions -> partitions.contains(partition)));

        transport.stop();
        executorService.shutdownNow();
    }

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

    @Test
    @DisplayName("DEFECT-6.1: Default consumer properties must disable auto-commit and use earliest offset reset")
    void defaultConsumerPropertiesMustDisableAutoCommit() {
        Properties props = KafkaInTransport.defaultConsumerProperties("localhost:9092", "test-group");

        assertEquals("false", props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
                "enable.auto.commit must be false: offsets committed by the auto-committer may point "
                        + "past records whose processing has not finished, losing them on crash");
        assertEquals("earliest", props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "auto.offset.reset must be earliest: a new consumer group must not silently "
                        + "skip the retained backlog");
    }

    @Test
    @DisplayName("DEFECT-6.1: Custom consumer properties default to at-least-once when not overridden")
    void customConsumerPropertiesDefaultToAtLeastOnce() {
        Properties userProps = new Properties();
        userProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaInTransport transport = new KafkaInTransport(userProps, "test-topic", "test-group");

        assertEquals("kafka", transport.name());
        // The properties object passed by the caller must not be mutated.
        assertFalse(userProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    @DisplayName("DEFECT-6.1: Offsets must not be committed until the processing future completes")
    void shouldNotCommitOffsetsUntilProcessingCompletes() throws Exception {
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

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        // A future that stays incomplete: processing is still running.
        CompletableFuture<Object> slowProcessing = new CompletableFuture<>();
        AsyncEventConsumer consumer = e -> slowProcessing;

        recordCommits();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);
        await().atMost(Duration.ofSeconds(5)).until(() -> !slowProcessing.isDone());
        await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).until(() -> true);

        assertTrue(committedBatches.isEmpty(),
                "No offsets may be committed while processing is still running");

        // Processing finishes — only now the offset may be committed.
        slowProcessing.complete(null);
        TopicPartition partition = new TopicPartition("test-topic", 0);
        await().atMost(Duration.ofSeconds(5)).until(() -> committedOffset(partition) == 1L);

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("DEFECT-6.1: A failed record must not be committed and must block later offsets of its partition")
    void shouldNotCommitFailedRecordOrLaterOffsets() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        JsonEventSerializer serializer = new JsonEventSerializer();

        ConsumerRecord<String, byte[]> failing = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-0", "m0"))
        );
        ConsumerRecord<String, byte[]> succeeding = new ConsumerRecord<>(
                "test-topic", 0, 1, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-1", "m1"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(failing, succeeding))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        Map<String, CompletableFuture<Object>> futures = new java.util.concurrent.ConcurrentHashMap<>();
        AsyncEventConsumer consumer = e -> futures.computeIfAbsent(((TestEvent) e).id, k -> new CompletableFuture<>());

        recordCommits();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);
        await().atMost(Duration.ofSeconds(5)).until(() -> futures.size() == 2);

        // The first record fails, the second one succeeds.
        futures.get("id-0").completeExceptionally(new IllegalStateException("handler failure"));
        futures.get("id-1").complete(null);

        await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).until(() -> true);

        assertTrue(committedBatches.isEmpty(),
                "A failed head record must block the commit of all later offsets of its partition");

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("DEFECT-6.1: Offsets must be committed contiguously — out-of-order completion does not skip a failed head")
    void shouldCommitOnlyContiguouslyCompletedOffsets() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        JsonEventSerializer serializer = new JsonEventSerializer();

        ConsumerRecord<String, byte[]> first = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-0", "m0"))
        );
        ConsumerRecord<String, byte[]> second = new ConsumerRecord<>(
                "test-topic", 0, 1, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-1", "m1"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(first, second))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        Map<String, CompletableFuture<Object>> futures = new java.util.concurrent.ConcurrentHashMap<>();
        AsyncEventConsumer consumer = e -> futures.computeIfAbsent(((TestEvent) e).id, k -> new CompletableFuture<>());

        recordCommits();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);
        await().atMost(Duration.ofSeconds(5)).until(() -> futures.size() == 2);

        // The later record completes first: nothing may be committed yet.
        futures.get("id-1").complete(null);
        await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).until(() -> true);
        assertTrue(committedBatches.isEmpty(),
                "Out-of-order completion must not commit an offset whose predecessor is unfinished");

        // The head record completes: the frontier advances past both records.
        futures.get("id-0").complete(null);
        TopicPartition partition = new TopicPartition("test-topic", 0);
        await().atMost(Duration.ofSeconds(5)).until(() -> committedOffset(partition) == 2L);

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("DEFECT-6.1: A poison pill record is skipped and its offset committed to avoid an endless redelivery loop")
    void shouldSkipPoisonRecordAndCommitItsOffset() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        byte[] malformedData = "{invalid json}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ConsumerRecord<String, byte[]> poison = new ConsumerRecord<>(
                "test-topic", 0, 0, "unknown", malformedData
        );
        ConsumerRecord<String, byte[]> valid = new ConsumerRecord<>(
                "test-topic", 0, 1, TestEvent.class.getName(),
                new JsonEventSerializer().serialize(TestEvent.create("id-1", "m1"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(poison, valid))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        TestEventConsumer consumer = new TestEventConsumer();

        recordCommits();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);

        TopicPartition partition = new TopicPartition("test-topic", 0);
        await().atMost(Duration.ofSeconds(5)).until(() -> committedOffset(partition) == 2L);
        assertEquals(1, consumer.getEvents().size());

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should trim topic names and ignore blank entries")
    void shouldTrimTopicNames() {
        assertEquals(List.of("topic-a", "topic-b"), KafkaInTransport.parseTopics(" topic-a , topic-b ,, "));
        assertEquals(List.of(), KafkaInTransport.parseTopics(" ,, "));
    }

    @Test
    @DisplayName("Should reject an empty topic configuration with a clear message")
    void shouldRejectEmptyTopics() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> new KafkaInTransport(mockConsumer, List.of(), executorService,
                            new EventSerializerFactory()));
            assertTrue(error.getMessage().contains("topic"));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    @DisplayName("Should reject a blank consumer group id with a clear message")
    void shouldRejectBlankConsumerGroup() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new KafkaInTransport("localhost:9092", "test-topic", ""));
        assertTrue(error.getMessage().contains("consumer-group"));
    }

    @Test
    @DisplayName("Should block a partition after a processing failure and never resume it in this session")
    void shouldBlockPartitionAfterFailedRecord() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        JsonEventSerializer serializer = new JsonEventSerializer();
        TopicPartition partition = new TopicPartition("test-topic", 0);

        ConsumerRecord<String, byte[]> failing = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-0", "m0"))
        );
        ConsumerRecord<String, byte[]> succeeding = new ConsumerRecord<>(
                "test-topic", 0, 1, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-1", "m1"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(partition, List.of(failing, succeeding))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());
        when(mockConsumer.assignment()).thenReturn(Set.of(partition));

        Map<String, CompletableFuture<Object>> futures = new java.util.concurrent.ConcurrentHashMap<>();
        AsyncEventConsumer consumer = e -> futures.computeIfAbsent(((TestEvent) e).id, k -> new CompletableFuture<>());

        recordCommits();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);
        await().atMost(Duration.ofSeconds(5)).until(() -> futures.size() == 2);

        futures.get("id-0").completeExceptionally(new IllegalStateException("handler failure"));
        futures.get("id-1").complete(null);

        verify(mockConsumer, timeout(5000)).pause(argThat(partitions -> partitions.contains(partition)));
        verify(mockConsumer, never()).resume(anyCollection());

        // Nothing from the blocked partition may be committed.
        assertTrue(committedBatches.isEmpty(),
                "Offsets of a blocked partition must not be committed in this session");

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should subscribe with a rebalance listener and tolerate assignment/revocation callbacks")
    void shouldRegisterRebalanceListener() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        when(mockConsumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );
        transport.start(e -> CompletableFuture.completedFuture(null));

        ArgumentCaptor<ConsumerRebalanceListener> listenerCaptor =
                ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
        verify(mockConsumer, timeout(5000))
                .subscribe(eq(List.of("test-topic")), listenerCaptor.capture());

        ConsumerRebalanceListener listener = listenerCaptor.getValue();
        TopicPartition partition = new TopicPartition("test-topic", 0);
        assertDoesNotThrow(() -> listener.onPartitionsAssigned(List.of(partition)));
        assertDoesNotThrow(() -> listener.onPartitionsRevoked(List.of(partition)));
        assertDoesNotThrow(() -> listener.onPartitionsLost(List.of(partition)));

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should unblock a failed partition by seeking to the failed offset and resuming it")
    void shouldUnblockBlockedPartition() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        JsonEventSerializer serializer = new JsonEventSerializer();
        TopicPartition partition = new TopicPartition("test-topic", 0);

        ConsumerRecord<String, byte[]> failing = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-0", "m0"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(partition, List.of(failing))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());
        when(mockConsumer.assignment()).thenReturn(Set.of(partition));

        CompletableFuture<Object> failedProcessing = new CompletableFuture<>();
        java.util.concurrent.atomic.AtomicInteger delivered =
                new java.util.concurrent.atomic.AtomicInteger();
        AsyncEventConsumer consumer = e -> {
            delivered.incrementAndGet();
            return failedProcessing;
        };

        recordCommits();
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory()
        );

        transport.start(consumer);
        await().atMost(Duration.ofSeconds(5)).until(() -> delivered.get() == 1);
        failedProcessing.completeExceptionally(new IllegalStateException("handler failure"));
        verify(mockConsumer, timeout(5000)).pause(argThat(partitions -> partitions.contains(partition)));

        transport.unblock(partition);

        verify(mockConsumer, timeout(5000)).seek(eq(partition), eq(0L));
        verify(mockConsumer, timeout(5000)).resume(argThat(partitions -> partitions.contains(partition)));

        transport.stop();
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should pause a full partition and resume it after the pending queue drains")
    void shouldApplyBackpressure() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        JsonEventSerializer serializer = new JsonEventSerializer();
        TopicPartition partition = new TopicPartition("test-topic", 0);

        ConsumerRecord<String, byte[]> first = new ConsumerRecord<>(
                "test-topic", 0, 0, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-0", "m0"))
        );
        ConsumerRecord<String, byte[]> second = new ConsumerRecord<>(
                "test-topic", 0, 1, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-1", "m1"))
        );
        ConsumerRecord<String, byte[]> third = new ConsumerRecord<>(
                "test-topic", 0, 2, TestEvent.class.getName(), serializer.serialize(TestEvent.create("id-2", "m2"))
        );
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(partition, List.of(first, second, third))
        );

        when(mockConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());
        when(mockConsumer.assignment()).thenReturn(Set.of(partition));

        List<CompletableFuture<Object>> inFlight = new java.util.concurrent.CopyOnWriteArrayList<>();
        AsyncEventConsumer consumer = e -> {
            CompletableFuture<Object> future = new CompletableFuture<>();
            inFlight.add(future);
            return future;
        };

        recordCommits();
        // Backpressure limit of 2: the third pending record pauses the partition.
        KafkaInTransport transport = new KafkaInTransport(
                mockConsumer, List.of("test-topic"), executorService, new EventSerializerFactory(), 2
        );

        transport.start(consumer);
        await().atMost(Duration.ofSeconds(5)).until(() -> inFlight.size() == 3);

        verify(mockConsumer, timeout(5000)).pause(argThat(partitions -> partitions.contains(partition)));

        // Drain the queue: the partition is resumed and the whole batch committed.
        inFlight.forEach(future -> future.complete(null));
        await().atMost(Duration.ofSeconds(5)).until(() -> committedOffset(partition) == 3L);
        verify(mockConsumer, timeout(5000)).resume(argThat(partitions -> partitions.contains(partition)));

        transport.stop();
        executorService.shutdownNow();
    }

    private void recordCommits() {
        // Lenient: several tests legitimately expect zero commits.
        lenient().doAnswer(invocation -> {
            committedBatches.add(new java.util.HashMap<>(invocation.getArgument(0)));
            return null;
        }).when(mockConsumer).commitSync(anyMap());
    }

    private long committedOffset(TopicPartition partition) {
        return committedBatches.stream()
                .map(batch -> batch.get(partition))
                .filter(java.util.Objects::nonNull)
                .mapToLong(OffsetAndMetadata::offset)
                .max()
                .orElse(-1L);
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

    /**
     * Consumer that records delivered events and completes the returned future
     * immediately, mimicking a fast synchronous handler.
     */
    static class TestEventConsumer implements AsyncEventConsumer {
        private final java.util.List<Event> events = new java.util.ArrayList<>();

        @Override
        public CompletableFuture<?> accept(Event event) {
            events.add(event);
            return CompletableFuture.completedFuture(null);
        }

        public List<Event> getEvents() {
            return events;
        }
    }
}
