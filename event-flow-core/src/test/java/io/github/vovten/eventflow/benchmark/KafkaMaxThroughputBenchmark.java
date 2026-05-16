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
import org.apache.kafka.clients.producer.ProducerConfig;
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
 * Kafka optimized benchmark - find max throughput.
 */
@Disabled("Run manually: mvn test -pl event-flow-core -Dtest=KafkaMaxThroughputBenchmark.java -DskipTests=false")
@DisplayName("Kafka Max Throughput Benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaMaxThroughputBenchmark {

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "192.168.1.39:9092");
    private static final int EVENT_COUNT = 1_000_000;

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
        EventTypeRegistry.allowClass(BenchmarkEvent.class);
    }

    @AfterEach
    void tearDown() {
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdownNow();
        }
    }

    private String createTopic() throws Exception {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        
        String topic = "max-throughput-" + System.nanoTime();
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        Thread.sleep(2000);
        return topic;
    }

    private void runBenchmark(String topic, int batchSize, int lingerMs, int maxRequestSize, int bufferMemory, 
                              int fetchMin, int fetchMax, int maxPollRecords, String compression) throws Exception {
        
        // Optimized producer config
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        
        // Batch settings
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(lingerMs));
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferMemory));
        producerProps.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(maxRequestSize));
        
        // Compression
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        
        // Performance
        producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        producerProps.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "1"); // Better performance than "all"
        producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false"); // Disable for max speed
        
        // Consumer settings
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(fetchMin));
        consumerProps.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(fetchMax));
        consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        
        JsonEventSerializer serializer = new JsonEventSerializer();

        try (KafkaOutTransport kafkaOutTransport = new KafkaOutTransport(producerProps, topic, serializer)) {
            EventPublisher publisher = event -> kafkaOutTransport.send(event)
                    .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(List.of(r)));

            BenchmarkEventHandler handler = new BenchmarkEventHandler();
            EventListenerRegistry registry = new EventListenerRegistry();
            registry.register(handler);

            String groupId = "max-" + System.nanoTime();
            Consumer<String, byte[]> consumer = createConsumer(BOOTSTRAP_SERVERS, groupId);
            
            try (KafkaInTransport kafkaInTransport = new KafkaInTransport(
                    consumer,
                    List.of(topic),
                    Executors.newSingleThreadExecutor(),
                    new io.github.vovten.eventflow.serialization.EventSerializerFactory())) {

                dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
                UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                        dispatcherExecutor, registry, List.of(kafkaInTransport)
                );
                dispatcher.start(event -> dispatcher.dispatch(event));

                Thread.sleep(3000);

                CountDownLatch latch = new CountDownLatch(EVENT_COUNT);
                handler.setLatch(latch);
                handler.reset();

                long startTime = System.nanoTime();

                System.out.println("  Publishing " + EVENT_COUNT + " events...");
                for (int i = 0; i < EVENT_COUNT; i++) {
                    publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
                    if (i > 0 && i % 100000 == 0) {
                        System.out.printf("    Published %,d...%n", i);
                    }
                }

                System.out.println("  Waiting for consumer...");
                boolean completed = latch.await(600, TimeUnit.SECONDS);
                long endTime = System.nanoTime();

                dispatcher.stop();

                double totalDuration = (endTime - startTime) / 1_000_000_000.0;
                double throughput = EVENT_COUNT / totalDuration;

                System.out.printf("  %s | %,d events | %,.0f/sec | %.2fs%n", 
                    String.format("BS=%d/L=%d/C=%s", batchSize/1024, lingerMs, compression),
                    handler.getProcessedCount(), throughput, totalDuration);

                assertEquals(EVENT_COUNT, handler.getProcessedCount(), "All events should be processed");
            }
        }
    }

    @Test
    @DisplayName("1M events - balanced config (128KB/20ms/lz4)")
    void shouldHandle1MEventsBalanced() throws Exception {
        String topic = createTopic();
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║              Kafka 1M Events - Balanced Config                ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        
        runBenchmark(topic, 131072, 20, 5242880, 268435456, 
                     1024, 134217728, 500, "lz4");
    }

    @Test
    @DisplayName("1M events - max throughput config")
    void shouldHandle1MEventsMaxThroughput() throws Exception {
        String topic = createTopic();
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║              Kafka 1M Events - Max Throughput                   ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        
        // Maximum throughput settings
        runBenchmark(topic, 262144, 50, 10485760, 536870912, 
                     1, 134217728, 10000, "lz4");
    }

    @Test
    @DisplayName("1M events - aggressive batching")
    void shouldHandle1MEventsAggressiveBatching() throws Exception {
        String topic = createTopic();
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║              Kafka 1M Events - Aggressive Batching              ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        
        // Aggressive batching with more partitions would help
        runBenchmark(topic, 524288, 100, 10485760, 1073741824, 
                     1, 52428800, 10000, "lz4");
    }

    public static class BenchmarkEvent extends AbstractTraceableEvent {
        private String id;
        private String message;

        public BenchmarkEvent() { super(); }
        public BenchmarkEvent(String id, String message) { super(); this.id = id; this.message = message; }
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        @Override public Class<?> type() { return BenchmarkEvent.class; }
    }

    public static class BenchmarkEventHandler {
        private final AtomicLong processedCount = new AtomicLong(0);
        private CountDownLatch latch;

        public void setLatch(CountDownLatch latch) { this.latch = latch; }
        public void reset() { processedCount.set(0); }
        public long getProcessedCount() { return processedCount.get(); }

        @EventListener
        public void onBenchmarkEvent(BenchmarkEvent event) {
            long count = processedCount.incrementAndGet();
            if (count % 100000 == 0) {
                System.out.printf("    Processed %,d...%n", count);
            }
            if (latch != null) latch.countDown();
        }
    }
}