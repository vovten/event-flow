package io.github.vovten.eventflow.benchmark;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.registry.EventListenerRegistry;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.serialization.EventTypeRegistry;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.SendResults;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performance benchmark for Kafka transport with default settings.
 * Run manually: mvn test -pl event-flow-core -Dtest=KafkaBenchmarkTest -Dkafka.bootstrap.servers=192.168.1.39:9092 -DskipTests=false
 */
@Disabled("Run manually: mvn test -pl event-flow-core -Dtest=KafkaBenchmarkTest -DskipTests=false")
@DisplayName("Kafka Benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaBenchmarkTest {

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
        BenchmarkLogConfig.configureForBenchmarks();
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

        String topic = "benchmark-1m-" + System.nanoTime();
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        Thread.sleep(2000);
        return topic;
    }

    @Test
    @DisplayName("1M events throughput benchmark")
    void shouldHandle1MEvents() throws Exception {
        String topic = createTopic();

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║          Kafka 1M Events Benchmark (Default Settings)       ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║ Server:     %-46s║%n", BOOTSTRAP_SERVERS);
        System.out.printf("║ Events:     %,d                                      ║%n", EVENT_COUNT);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // Default KafkaOutTransport settings (128KB batch, 20ms linger, lz4)
        JsonEventSerializer serializer = new JsonEventSerializer();

        try (KafkaOutTransport kafkaOutTransport = new KafkaOutTransport(
                createProperties(),
                topic, serializer)) {

            EventPublisher publisher = event -> kafkaOutTransport.send(event)
                    .thenApply(r -> SendResults.of(List.of(r)));

            BenchmarkEventHandler handler = new BenchmarkEventHandler();
            EventListenerRegistry registry = new EventListenerRegistry();
            registry.register(handler);

            String groupId = "benchmark-1m-" + System.nanoTime();
            try (KafkaInTransport kafkaInTransport = new KafkaInTransport(
                    createConsumer(BOOTSTRAP_SERVERS, groupId),
                    List.of(topic),
                    Executors.newSingleThreadExecutor(),
                    new EventSerializerFactory())) {

                dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
                UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                        dispatcherExecutor, registry, List.of(kafkaInTransport)
                );
                dispatcher.start(event -> dispatcher.dispatch(event));

                // Wait for consumer to be ready
                Thread.sleep(3000);

                CountDownLatch latch = new CountDownLatch(EVENT_COUNT);
                handler.setLatch(latch);
                handler.reset();

                long startTime = System.nanoTime();

                System.out.println("  Publishing " + EVENT_COUNT + " events...");
                for (int i = 0; i < EVENT_COUNT; i++) {
                    publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
                    if (i > 0 && i % 250000 == 0) {
                        System.out.printf("    Published %,d...%n", i);
                    }
                }

                long publishEndTime = System.nanoTime();
                System.out.printf("  Published in %.2f seconds%n", (publishEndTime - startTime) / 1_000_000_000.0);
                System.out.println("  Waiting for consumer...");

                boolean completedFinal = latch.await(600, TimeUnit.SECONDS);
                long endTime = System.nanoTime();
                boolean completed = completedFinal;

                dispatcher.stop();

                assertEquals(EVENT_COUNT, handler.getProcessedCount(), "All events should be processed");

                double totalDuration = (System.nanoTime() - startTime) / 1_000_000_000.0;
                double publishDuration = (publishEndTime - startTime) / 1_000_000_000.0;
                double processDuration = (System.nanoTime() - publishEndTime) / 1_000_000_000.0;
                double throughput = EVENT_COUNT / totalDuration;

                printResult(handler, throughput, totalDuration, publishDuration, processDuration);

                assertTrue(completed, "All events should be processed within timeout");
            }
        }
    }

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        return properties;
    }

    private void printResult(BenchmarkEventHandler handler, double throughput, double totalDuration, double publishDuration, double processDuration) {
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Results                                   ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║ Received:   %,12d                                      ║%n", handler.getProcessedCount());
        System.out.printf("║ Throughput: %12.0f events/sec                         ║%n", throughput);
        System.out.printf("║ Total time: %12.3f seconds                             ║%n", totalDuration);
        System.out.printf("║ Publish:    %12.3f seconds                             ║%n", publishDuration);
        System.out.printf("║ Process:    %12.3f seconds                             ║%n", processDuration);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    static class BenchmarkEvent extends AbstractTraceableEvent {
        private String id;
        private String message;

        BenchmarkEvent() {
            super();
        }

        BenchmarkEvent(String id, String message) {
            super();
            this.id = id;
            this.message = message;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public Class<?> type() {
            return BenchmarkEvent.class;
        }
    }

    static class BenchmarkEventHandler {
        private final AtomicLong processedCount = new AtomicLong(0);
        private CountDownLatch latch;

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        public void reset() {
            processedCount.set(0);
        }

        public long getProcessedCount() {
            return processedCount.get();
        }

        @EventListener
        public void onBenchmarkEvent(BenchmarkEvent event) {
            long count = processedCount.incrementAndGet();
            if (count % 250000 == 0) {
                System.out.printf("    Processed %,d...%n", count);
            }
            if (latch != null) {
                latch.countDown();
            }
        }
    }
}