package com.github.vovten.eventflow.event;

import com.github.vovten.eventflow.event.dispatcher.ExternalEventDispatcher;
import com.github.vovten.eventflow.event.publisher.ExternalEventPublisher;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Integration tests for ExternalEventPublisher with Embedded Kafka
 */
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" },
    topics = { "test-events" }
)
class ExternalPublisherDispatcherIntegrationTest {

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaBrokers;
    @Autowired
    ApplicationContext applicationContext;
    @Autowired
    TestEventListener eventListener;

    private ExternalEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new ExternalEventPublisher(
                createKafkaTemplate(),
                "test-events"
        );
        ExternalEventDispatcher dispatcher = new ExternalEventDispatcher(
                createDispatcherConsumer(),
                List.of("test-events"),
                Executors.newSingleThreadExecutor(),
                executorService(),
                "com.github.vovten.eventflow.event.publisher"
        );
        dispatcher.start();
        dispatcher.onApplicationEvent(new ContextRefreshedEvent(applicationContext));
    }

    @Test
    @DisplayName("Should publish event to Kafka topic")
    void shouldPublishEventToKafkaTopic() throws InterruptedException {
        // arrange
        TestEvent testEvent = new TestEvent("test-id-123");

        // arrange act
        publisher.publish(testEvent);

        // assert
        Thread.sleep(3000L);
        Assertions.assertEquals("test-id-123", eventListener.getResult());
    }

    private KafkaTemplate<String, String> createKafkaTemplate() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(properties);
        return new KafkaTemplate<>(producerFactory);
    }

    private Consumer<String, String> createDispatcherConsumer() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dispatcher-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(properties);
        return factory.createConsumer();
    }

    private static ExecutorService executorService() {
        return new ExecutorService() {
            @Override
            public void shutdown() {

            }

            @Override
            public List<Runnable> shutdownNow() {
                return List.of();
            }

            @Override
            public boolean isShutdown() {
                return false;
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return false;
            }

            @Override
            public <T> Future<T> submit(Callable<T> task) {
                return null;
            }

            @Override
            public <T> Future<T> submit(Runnable task, T result) {
                return null;
            }

            @Override
            public Future<?> submit(Runnable task) {
                if (task == null) throw new NullPointerException();
                RunnableFuture<Void> futureTask = new FutureTask(task, null);
                execute(futureTask);
                return futureTask;
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
                return List.of();
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
                return List.of();
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
                return null;
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return null;
            }

            @Override
            public void execute(Runnable command) {
                command.run();
            }
        };
    }
}
