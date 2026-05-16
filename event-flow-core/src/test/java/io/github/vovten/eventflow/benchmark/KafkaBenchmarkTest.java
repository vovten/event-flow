package io.github.vovten.eventflow.benchmark;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.registry.EventListenerRegistry;
import io.github.vovten.eventflow.serialization.EventTypeRegistry;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.incoming.KafkaInTransport;
import io.github.vovten.eventflow.transport.outgoing.KafkaOutTransport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Performance benchmark for Kafka transport.
 * Requires a running Kafka instance (skip if not available).
 */
@Disabled("Run manually: mvn test -pl event-flow-core -Dtest=KafkaBenchmarkTest.java -DskipTests=false")
@DisplayName("Kafka Benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaBenchmarkTest {

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    private static final String TOPIC = "ef-benchmark-" + System.currentTimeMillis();

    private static final AtomicLong SENT_COUNT = new AtomicLong(0);
    private static final AtomicLong RECEIVED_COUNT = new AtomicLong(0);

    private ExecutorService dispatcherExecutor;

    private static Consumer<String, byte[]> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    static {
        // Allow BenchmarkEvent for deserialization
        EventTypeRegistry.allowClass(BenchmarkEvent.class);
    }

    @BeforeAll
    static void setupTopic() {
        try {
            Properties props = new Properties();
            props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
            props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
            
            try (AdminClient admin = AdminClient.create(props)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
                System.out.println("Topic ready: " + TOPIC);
            }
        } catch (Exception e) {
            System.out.println("Topic setup: " + e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdownNow();
        }
        System.out.println("Sent: " + SENT_COUNT.get() + ", Received: " + RECEIVED_COUNT.get());
    }

    @Nested
    @Disabled("Run manually: mvn test -pl event-flow-core -Dtest=KafkaBenchmarkTest.java#methodName -DskipTests=false")
@DisplayName("Kafka Benchmark")
    class KafkaTransportBenchmark {

        @Test
        @DisplayName("Should send 10 events and receive them back")
        void shouldSendAndReceiveEventsThroughKafka() throws Exception {
            int eventCount = 10;

            // Use default JSON serializer
            JsonEventSerializer serializer = new JsonEventSerializer();

            // Create Kafka OUT transport
            Properties outProps = new Properties();
            outProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
            
            try (KafkaOutTransport kafkaOutTransport = new KafkaOutTransport(outProps, TOPIC, serializer)) {
                EventPublisher publisher = event -> kafkaOutTransport.send(event)
                        .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(List.of(r)));

                // Handler
                BenchmarkEventHandler handler = new BenchmarkEventHandler();
                EventListenerRegistry registry = new EventListenerRegistry();
                registry.register(handler);

                // Create Kafka IN transport with same serializer
                String groupId = "benchmark-group-" + System.currentTimeMillis();
                try (KafkaInTransport kafkaInTransport = new KafkaInTransport(
                        createConsumer(BOOTSTRAP_SERVERS, groupId),
                        List.of(TOPIC),
                        Executors.newSingleThreadExecutor(),
                        new io.github.vovten.eventflow.serialization.EventSerializerFactory())) {
                    
                    dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
                    UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                            dispatcherExecutor, registry, List.of(kafkaInTransport)
                    );
                    dispatcher.start(event -> dispatcher.dispatch(event));

                    // Wait for consumer to be ready and committed offset
                    Thread.sleep(3000);

                    System.out.println("Starting to send " + eventCount + " events...");

                    CountDownLatch latch = new CountDownLatch(eventCount);
                    handler.setLatch(latch);
                    handler.reset();

                    long startTime = System.nanoTime();

                    for (int i = 0; i < eventCount; i++) {
                        publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
                        SENT_COUNT.incrementAndGet();
                        Thread.sleep(20);
                    }

                    // Wait up to 60 seconds for events
                    boolean completed = latch.await(60, TimeUnit.SECONDS);
                    long endTime = System.nanoTime();

                    dispatcher.stop();

                    double totalDuration = (endTime - startTime) / 1_000_000_000.0;

                    System.out.println("\n========================================");
                    System.out.println("Kafka Benchmark Results");
                    System.out.println("========================================");
                    System.out.printf("Total events:       %,d%n", eventCount);
                    System.out.printf("Received:           %,d%n", handler.getProcessedCount());
                    System.out.printf("Total duration:      %.3f seconds%n", totalDuration);
                    if (handler.getProcessedCount() > 0) {
                        System.out.printf("Throughput:          %,.0f events/second%n", eventCount / totalDuration);
                    }
                    System.out.println("========================================\n");

                    assertEquals(eventCount, handler.getProcessedCount(), "All events should be processed");
                }
            }
        }

        @Test
        @DisplayName("Should handle 1k events")
        void shouldHandle1kEvents() throws Exception {
            int eventCount = 1000;

            JsonEventSerializer serializer = new JsonEventSerializer();

            Properties outProps = new Properties();
            outProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
            
            String topic1k = TOPIC + "-1k";
            try {
                Properties adminProps = new Properties();
                adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
                try (AdminClient admin = AdminClient.create(adminProps)) {
                    admin.createTopics(List.of(new NewTopic(topic1k, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
                }
            } catch (Exception e) { /* already exists */ }
            
            try (KafkaOutTransport kafkaOutTransport = new KafkaOutTransport(outProps, topic1k, serializer)) {
                EventPublisher publisher = event -> kafkaOutTransport.send(event)
                        .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(List.of(r)));

                BenchmarkEventHandler handler = new BenchmarkEventHandler();
                EventListenerRegistry registry = new EventListenerRegistry();
                registry.register(handler);

                String groupId = "benchmark-group-1k-" + System.currentTimeMillis();
                try (KafkaInTransport kafkaInTransport = new KafkaInTransport(
                        createConsumer(BOOTSTRAP_SERVERS, groupId),
                        List.of(topic1k),
                        Executors.newSingleThreadExecutor(),
                        new io.github.vovten.eventflow.serialization.EventSerializerFactory())) {
                    
                    dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
                    UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                            dispatcherExecutor, registry, List.of(kafkaInTransport)
                    );
                    dispatcher.start(event -> dispatcher.dispatch(event));

                    Thread.sleep(3000);

                    CountDownLatch latch = new CountDownLatch(eventCount);
                    handler.setLatch(latch);
                    handler.reset();

                    long startTime = System.nanoTime();

                    for (int i = 0; i < eventCount; i++) {
                        publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
                    }

                    boolean completed = latch.await(120, TimeUnit.SECONDS);
                    long endTime = System.nanoTime();

                    dispatcher.stop();

                    double totalDuration = (endTime - startTime) / 1_000_000_000.0;
                    double throughput = eventCount / totalDuration;

                    System.out.println("\n========================================");
                    System.out.println("Kafka 1k Events Benchmark");
                    System.out.println("========================================");
                    System.out.printf("Total events:       %,d%n", eventCount);
                    System.out.printf("Received:           %,d%n", handler.getProcessedCount());
                    System.out.printf("Total duration:      %.3f seconds%n", totalDuration);
                    System.out.printf("Throughput:          %,.0f events/second%n", throughput);
                    System.out.println("========================================\n");

                    assertEquals(eventCount, handler.getProcessedCount(), "All events should be processed");
                }
            }
        }
    }

    public static class BenchmarkEvent extends AbstractTraceableEvent {
        private String id;
        private String message;

        public BenchmarkEvent() {
            super();
        }

        public BenchmarkEvent(String id, String message) {
            super();
            this.id = id;
            this.message = message;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        @Override
        public Class<?> type() { return BenchmarkEvent.class; }
    }

    public static class BenchmarkEventHandler {
        private final AtomicLong processedCount = new AtomicLong(0);
        private CountDownLatch latch;

        public void setLatch(CountDownLatch latch) { this.latch = latch; }
        public void reset() { processedCount.set(0); }
        public long getProcessedCount() { return processedCount.get(); }

        @EventListener
        public void onBenchmarkEvent(BenchmarkEvent event) {
            processedCount.incrementAndGet();
            RECEIVED_COUNT.incrementAndGet();
            if (processedCount.get() <= 5) {
                System.out.println(">>> Received event #" + processedCount.get() + ": " + event.getId());
            }
            if (latch != null) latch.countDown();
        }
    }
}