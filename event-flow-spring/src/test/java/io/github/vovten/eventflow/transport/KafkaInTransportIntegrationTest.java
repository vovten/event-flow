package io.github.vovten.eventflow.transport;

import io.github.vovten.eventflow.autoconfig.EventFlowDisabledAutoConfiguration;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.incoming.KafkaInTransport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests for {@link KafkaInTransport} against a real embedded Kafka
 * broker (no mocks): real offsets, real commits, real redelivery and rebalance.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@SpringBootTest(properties = "event-flow.enabled=false")
@ImportAutoConfiguration(exclude = EventFlowDisabledAutoConfiguration.class)
@EmbeddedKafka(
        partitions = 1,
        topics = {"in-transport-bootstrap"},
        brokerProperties = {"group.initial.rebalance.delay.ms=0"}
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("KafkaInTransport Integration Tests")
class KafkaInTransportIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private final JsonEventSerializer serializer = new JsonEventSerializer();
    private final List<KafkaInTransport> transports = new CopyOnWriteArrayList<>();

    private String topic;
    private String groupId;

    @BeforeEach
    void setUp() {
        topic = "in-transport-" + UUID.randomUUID();
        groupId = "in-transport-group-" + UUID.randomUUID();
        embeddedKafka.addTopics(new NewTopic(topic, 1, (short) 1));
    }

    @AfterEach
    void tearDown() {
        transports.forEach(KafkaInTransport::stop);
        transports.clear();
    }

    @Test
    @DisplayName("Should not commit a failed offset and re-deliver from it after unblock")
    void shouldBlockFailedRecordAndResumeFromFailedOffsetAfterUnblock() {
        produce(serializedEvent("a"));
        produce(serializedEvent("b"));

        Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();
        CompletableFuture<Object> failing = new CompletableFuture<>();
        AtomicInteger failuresLeft = new AtomicInteger(1);
        AsyncEventConsumer consumer = event -> {
            String id = ((IntegrationEvent) event).id;
            deliveries.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
            if ("a".equals(id) && failuresLeft.getAndDecrement() > 0) {
                return failing;
            }
            return CompletableFuture.completedFuture(null);
        };

        KafkaInTransport transport = start(consumer);

        awaitUntil(() -> deliveries.getOrDefault("a", new AtomicInteger()).get() >= 1);
        failing.completeExceptionally(new IllegalStateException("deliberate failure"));

        // The failed head record blocks the partition: nothing is committed.
        sleep(700);
        assertEquals(-1L, committedOffset(),
                "A failed record must block the commit of the whole partition");

        transport.unblock(new TopicPartition(topic, 0));

        awaitUntil(() -> deliveries.getOrDefault("a", new AtomicInteger()).get() >= 2);
        awaitUntil(() -> deliveries.getOrDefault("b", new AtomicInteger()).get() >= 1);
        awaitUntil(() -> committedOffset() == 2L);
    }

    @Test
    @DisplayName("Should commit an offset only after the processing future completes")
    void shouldCommitOffsetOnlyAfterProcessingCompletes() {
        produce(serializedEvent("slow"));

        CompletableFuture<Object> processing = new CompletableFuture<>();
        AtomicInteger delivered = new AtomicInteger();
        AsyncEventConsumer consumer = event -> {
            delivered.incrementAndGet();
            return processing;
        };

        start(consumer);

        awaitUntil(() -> delivered.get() == 1);
        sleep(700);
        assertEquals(-1L, committedOffset(),
                "No offset may be committed while processing is still running");

        processing.complete(null);
        awaitUntil(() -> committedOffset() == 1L);
    }

    @Test
    @DisplayName("Should skip a poison pill and process the following record")
    void shouldSkipPoisonPillAndProcessFollowingRecord() {
        produce("{not-a-valid-json".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        produce(serializedEvent("after-poison"));

        Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();
        AsyncEventConsumer consumer = event -> {
            String id = ((IntegrationEvent) event).id;
            deliveries.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
            return CompletableFuture.completedFuture(null);
        };

        start(consumer);

        awaitUntil(() -> deliveries.getOrDefault("after-poison", new AtomicInteger()).get() == 1);
        awaitUntil(() -> committedOffset() == 2L);
    }

    @Test
    @DisplayName("Should not commit pending records when the transport stops")
    void shouldNotCommitPendingRecordOnStop() {
        produce(serializedEvent("pending"));

        CompletableFuture<Object> processing = new CompletableFuture<>();
        AtomicInteger delivered = new AtomicInteger();
        AsyncEventConsumer consumer = event -> {
            delivered.incrementAndGet();
            return processing;
        };

        KafkaInTransport transport = start(consumer);

        awaitUntil(() -> delivered.get() == 1);
        transport.stop();
        transports.remove(transport);

        assertEquals(-1L, committedOffset(),
                "A pending record must not be committed on shutdown");
        processing.complete(null);

        // Give a (hypothetically still running) loop a chance to commit; there is none.
        sleep(300);
        assertEquals(-1L, committedOffset());
    }

    @Test
    @DisplayName("Should treat a null processing future as a failure and not commit the record")
    void shouldBlockPartitionOnNullProcessingFuture() {
        produce(serializedEvent("null-future"));

        start(event -> null);

        sleep(1000);
        assertEquals(-1L, committedOffset(),
                "A null processing future must not be treated as successful processing");
    }

    @Test
    @DisplayName("Should skip a tombstone record and process the following record")
    void shouldSkipTombstoneAndProcessFollowingRecord() {
        produce(null);
        produce(serializedEvent("after-tombstone"));

        AtomicInteger deliveries = new AtomicInteger();
        AsyncEventConsumer consumer = event -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        };

        start(consumer);

        awaitUntil(() -> deliveries.get() == 1);
        awaitUntil(() -> committedOffset() == 2L);
    }

    @Test
    @DisplayName("Should commit the completed frontier on revoke and let the next owner re-deliver only the pending record")
    void shouldCommitCompletedFrontierOnRebalance() {
        produce(serializedEvent("m1"));
        produce(serializedEvent("m2"));

        Map<String, AtomicInteger> firstDeliveries = new ConcurrentHashMap<>();
        CompletableFuture<Object> pendingProcessing = new CompletableFuture<>();
        AsyncEventConsumer firstConsumer = event -> {
            String id = ((IntegrationEvent) event).id;
            firstDeliveries.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
            if ("m2".equals(id)) {
                return pendingProcessing;
            }
            return CompletableFuture.completedFuture(null);
        };

        KafkaInTransport firstTransport = start(firstConsumer);
        awaitUntil(() -> firstDeliveries.getOrDefault("m1", new AtomicInteger()).get() >= 1
                && firstDeliveries.getOrDefault("m2", new AtomicInteger()).get() >= 1);

        // m1 completed, m2 is still pending: the transport commits offset 1.
        awaitUntil(() -> committedOffset() == 1L);

        // The partition is revoked (consumer leaves the group). The completed
        // frontier must already be committed, so the next owner starts at 1.
        firstTransport.stop();
        transports.remove(firstTransport);

        Map<String, AtomicInteger> secondDeliveries = new ConcurrentHashMap<>();
        AsyncEventConsumer secondConsumer = event -> {
            String id = ((IntegrationEvent) event).id;
            secondDeliveries.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
            return CompletableFuture.completedFuture(null);
        };
        start(secondConsumer);

        // Only the pending record is re-delivered; m1 must not be processed again.
        awaitUntil(() -> secondDeliveries.getOrDefault("m2", new AtomicInteger()).get() >= 1);
        assertEquals(0, secondDeliveries.getOrDefault("m1", new AtomicInteger()).get(),
                "A committed record must not be re-delivered to the next owner");
        awaitUntil(() -> committedOffset() == 2L);
    }

    private KafkaInTransport start(AsyncEventConsumer consumer) {
        KafkaInTransport transport = createTransport();
        transports.add(transport);
        transport.start(consumer);
        return transport;
    }

    private KafkaInTransport createTransport() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        // Faster rebalance detection for the rebalance test.
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        return new KafkaInTransport(properties, topic, groupId, new EventSerializerFactory());
    }

    private byte[] serializedEvent(String id) {
        return serializer.serialize(new IntegrationEvent(id));
    }

    private void produce(byte[] value) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(topic, value)).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to produce test record", e);
        }
    }

    private long committedOffset() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        try (AdminClient admin = AdminClient.create(properties)) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);
            OffsetAndMetadata committed = offsets.get(new TopicPartition(topic, 0));
            return committed == null ? -1L : committed.offset();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                return -1L;
            }
            throw new IllegalStateException("Failed to read committed offsets", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading committed offsets", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Timed out while reading committed offsets", e);
        }
    }

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for the condition", e);
            }
        }
        fail("Condition was not met within " + TIMEOUT);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Event used by the integration tests. Living in the event-flow package keeps
     * it inside the default deserialization allow-list.
     */
    public static class IntegrationEvent extends AbstractTraceableEvent {

        public String id;

        IntegrationEvent() {
            super();
        }

        IntegrationEvent(String id) {
            super();
            this.id = id;
        }

        @Override
        public Class<? extends Event> type() {
            return IntegrationEvent.class;
        }
    }
}
