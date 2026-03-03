package com.github.vovten.eventflow.event.publisher;

import com.github.vovten.eventflow.event.EventBus;
import com.github.vovten.eventflow.event.test.ExternalTestEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ExternalEventPublisher with Embedded Kafka
 */
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" },
    topics = { "test-events" }
)
class ExternalEventPublisherIntegrationTest {

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaBrokers;
    
    private ExternalEventPublisher publisher;
    private org.apache.kafka.clients.consumer.Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        publisher = new ExternalEventPublisher(kafkaTemplate, "test-events");
        
        // Create consumer
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
    }

    @Test
    @DisplayName("Should publish event to Kafka topic")
    void shouldPublishEventToKafkaTopic() {
        // given
        ExternalTestEvent event = ExternalTestEvent.create("Test payload for Kafka");
        consumer.subscribe(java.util.Collections.singletonList("test-events"));

        // when
        publisher.publish(event);

        // then - poll with timeout
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
            consumer, "test-events", Duration.ofSeconds(5));
        
        assertNotNull(record.value());
        assertTrue(record.value().contains("Test payload for Kafka"), "Message should contain payload");
        assertTrue(record.value().contains("ExternalTestEvent"), "Message should contain event type");
    }

    @Test
    @DisplayName("Should return EXTERNAL event bus")
    void shouldReturnExternalEventBus() {
        // when
        EventBus eventBus = publisher.eventBus();

        // then
        assertEquals(EventBus.EXTERNAL, eventBus);
    }
}
