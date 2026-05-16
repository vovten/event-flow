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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Kafka parallel producer benchmark.
 */
@Disabled("Run manually: mvn test -pl event-flow-core -Dtest=KafkaParallelProducerBenchmark.java -DskipTests=false")
@DisplayName("Kafka Parallel Producer Benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaParallelProducerBenchmark {

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "192.168.1.39:9092");
    private static final int EVENT_COUNT = 1_000_000;
    private static final int PRODUCER_COUNT = 4;

    private ExecutorService dispatcherExecutor;
    private ExecutorService producerExecutor;

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
        if (dispatcherExecutor != null) dispatcherExecutor.shutdownNow();
        if (producerExecutor != null) producerExecutor.shutdownNow();
    }

    private String createTopic() throws Exception {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        
        String topic = "parallel-" + System.nanoTime();
        try (AdminClient admin = AdminClient.create(props)) {
            // Create topic with more partitions for parallel consumers
            admin.createTopics(List.of(new NewTopic(topic, 6, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        Thread.sleep(2000);
        return topic;
    }

    private void runParallelBenchmark(String topic, int producerCount) throws Exception {
        
        // Create multiple producers
        List<KafkaOutTransport> producers = new ArrayList<>();
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5"); // Lower linger for parallel
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "268435456");
        producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        producerProps.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        JsonEventSerializer serializer = new JsonEventSerializer();

        for (int i = 0; i < producerCount; i++) {
            producers.add(new KafkaOutTransport(new Properties(producerProps), topic, serializer));
        }

        // Create publisher that round-robins across producers
        AtomicLong publisherIndex = new AtomicLong(0);
        EventPublisher publisher = event -> {
            int idx = (int) (publisherIndex.getAndIncrement() % producerCount);
            return producers.get(idx).send(event)
                    .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(List.of(r)));
        };

        BenchmarkEventHandler handler = new BenchmarkEventHandler();
        EventListenerRegistry registry = new EventListenerRegistry();
        registry.register(handler);

        String groupId = "parallel-" + System.nanoTime();
        try (KafkaInTransport kafkaInTransport = new KafkaInTransport(
                createConsumer(BOOTSTRAP_SERVERS, groupId),
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

            producerExecutor = Executors.newFixedThreadPool(producerCount);
            long startTime = System.nanoTime();

            System.out.println("  Publishing " + EVENT_COUNT + " events with " + producerCount + " producers...");
            
            // Distribute events across producers
            int eventsPerProducer = EVENT_COUNT / producerCount;
            CountDownLatch producerLatch = new CountDownLatch(producerCount);
            
            for (int p = 0; p < producerCount; p++) {
                final int producerId = p;
                final int startIdx = producerId * eventsPerProducer;
                final int endIdx = (p == producerCount - 1) ? EVENT_COUNT : startIdx + eventsPerProducer;
                
                producerExecutor.submit(() -> {
                    try {
                        for (int i = startIdx; i < endIdx; i++) {
                            publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
                        }
                    } finally {
                        producerLatch.countDown();
                    }
                });
            }

            System.out.println("  Waiting for " + producerCount + " producers to finish...");
            producerLatch.await(300, TimeUnit.SECONDS);

            System.out.println("  Waiting for consumer...");
            boolean completed = latch.await(600, TimeUnit.SECONDS);
            long endTime = System.nanoTime();

            dispatcher.stop();

            double totalDuration = (endTime - startTime) / 1_000_000_000.0;
            double throughput = EVENT_COUNT / totalDuration;

            System.out.printf("  %d producers | %,d events | %,.0f/sec | %.2fs%n", 
                producerCount, handler.getProcessedCount(), throughput, totalDuration);

            assertEquals(EVENT_COUNT, handler.getProcessedCount(), "All events should be processed");
        }

        // Close producers
        for (KafkaOutTransport producer : producers) {
            producer.close();
        }
    }

    @Test
    @DisplayName("1M events - 1 producer (baseline)")
    void shouldHandle1MEvents1Producer() throws Exception {
        String topic = createTopic();
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║              Kafka 1M - 1 Producer (Baseline)                     ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        
        runParallelBenchmark(topic, 1);
    }

    @Test
    @DisplayName("1M events - 2 producers")
    void shouldHandle1MEvents2Producers() throws Exception {
        String topic = createTopic();
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║              Kafka 1M - 2 Parallel Producers                     ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        
        runParallelBenchmark(topic, 2);
    }

    @Test
    @DisplayName("1M events - 4 producers")
    void shouldHandle1MEvents4Producers() throws Exception {
        String topic = createTopic();
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║              Kafka 1M - 4 Parallel Producers                    ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        
        runParallelBenchmark(topic, 4);
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