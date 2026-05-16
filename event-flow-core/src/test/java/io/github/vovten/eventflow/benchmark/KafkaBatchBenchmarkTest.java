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
 * Kafka batch settings benchmark.
 */
@Disabled("Run manually: mvn test -pl event-flow-core -Dtest=KafkaBatchBenchmarkTest.java -DskipTests=false")
@DisplayName("Kafka Batch Benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaBatchBenchmarkTest {

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "192.168.1.39:9092");
    private static final int EVENT_COUNT = 10_000;

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

    private String createTopic(String prefix) throws Exception {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        
        String topic = prefix + "-" + System.nanoTime();
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        Thread.sleep(1000); // Wait for topic creation to complete
        return topic;
    }

    private void runBenchmark(String topic, int batchSize, int lingerMs) throws Exception {
        // Producer config
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(lingerMs));
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");
        producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        producerProps.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        JsonEventSerializer serializer = new JsonEventSerializer();

        try (KafkaOutTransport kafkaOutTransport = new KafkaOutTransport(producerProps, topic, serializer)) {
            EventPublisher publisher = event -> kafkaOutTransport.send(event)
                    .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(List.of(r)));

            BenchmarkEventHandler handler = new BenchmarkEventHandler();
            EventListenerRegistry registry = new EventListenerRegistry();
            registry.register(handler);

            String groupId = "batch-" + System.nanoTime();
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

                Thread.sleep(2000);

                CountDownLatch latch = new CountDownLatch(EVENT_COUNT);
                handler.setLatch(latch);
                handler.reset();

                long startTime = System.nanoTime();

                for (int i = 0; i < EVENT_COUNT; i++) {
                    publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
                }

                boolean completed = latch.await(120, TimeUnit.SECONDS);
                long endTime = System.nanoTime();

                dispatcher.stop();

                double totalDuration = (endTime - startTime) / 1_000_000_000.0;
                double throughput = EVENT_COUNT / totalDuration;
                double latencyAvg = (totalDuration / EVENT_COUNT) * 1000;

                String config = batchSize / 1024 + "KB/" + lingerMs + "ms";
                System.out.printf("  %-12s | %7d | %12.0f | %.4f%n", 
                    config, handler.getProcessedCount(), throughput, latencyAvg);

                assertEquals(EVENT_COUNT, handler.getProcessedCount(), "All events should be processed");
            }
        }
    }

    @Test
    @DisplayName("Compare batch configurations: Default vs Optimized")
    void shouldCompareBatchConfigurations() throws Exception {
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║           Kafka Batch Benchmark (10k events)              ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║ Config        │   Events │    Throughput │ Latency      ║");
        System.out.println("╠═══════════════╪═══════════╪═══════════════╪══════════════╣");
        
        String t1 = createTopic("batch-default");
        Thread.sleep(500);
        runBenchmark(t1, 16384, 0);
        
        Thread.sleep(1000);
        
        String t2 = createTopic("batch-optimized");
        Thread.sleep(500);
        runBenchmark(t2, 131072, 20);
        
        System.out.println("╚═══════════════╧═══════════╧═══════════════╧══════════════╝");
    }

    @Test
    @DisplayName("100k events with optimized config")
    void shouldHandle100kEvents() throws Exception {
        String topic = createTopic("batch-100k");
        Thread.sleep(500);
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║           Kafka 100k Events Benchmark                       ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "268435456");
        producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        producerProps.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        JsonEventSerializer serializer = new JsonEventSerializer();
        int eventCount = 100_000;

        try (KafkaOutTransport kafkaOutTransport = new KafkaOutTransport(producerProps, topic, serializer)) {
            EventPublisher publisher = event -> kafkaOutTransport.send(event)
                    .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(List.of(r)));

            BenchmarkEventHandler handler = new BenchmarkEventHandler();
            EventListenerRegistry registry = new EventListenerRegistry();
            registry.register(handler);

            String groupId = "batch-50k-" + System.nanoTime();
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

                Thread.sleep(2000);

                CountDownLatch latch = new CountDownLatch(eventCount);
                handler.setLatch(latch);
                handler.reset();

                long startTime = System.nanoTime();

                for (int i = 0; i < eventCount; i++) {
                    publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
                }

                boolean completed = latch.await(300, TimeUnit.SECONDS);
                long endTime = System.nanoTime();

                dispatcher.stop();

                double totalDuration = (endTime - startTime) / 1_000_000_000.0;
                double throughput = eventCount / totalDuration;

                System.out.printf("║ Events:     %,8d                                      ║%n", eventCount);
                System.out.printf("║ Received:   %,8d                                      ║%n", handler.getProcessedCount());
                System.out.printf("║ Throughput: %12.0f events/sec                       ║%n", throughput);
                System.out.printf("║ Duration:   %12.2f seconds                           ║%n", totalDuration);
                System.out.println("╚══════════════════════════════════════════════════════════════╝");

                assertEquals(eventCount, handler.getProcessedCount(), "All " + eventCount + " events should be processed");
            }
        }
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
            processedCount.incrementAndGet();
            if (latch != null) latch.countDown();
        }
    }
}