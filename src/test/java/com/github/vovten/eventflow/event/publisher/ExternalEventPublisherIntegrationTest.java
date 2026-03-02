package com.github.vovten.eventflow.event.publisher;

import com.github.vovten.eventflow.event.EventBus;
import com.github.vovten.eventflow.event.test.ExternalTestEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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
@TestPropertySource(properties = {
    "event.external.publisher.enabled=true",
    "event.external.publisher.topic=test-events",
    "kafka.bootstrap.servers=localhost:9092"
})
@ActiveProfiles("test")
@DirtiesContext
class ExternalEventPublisherIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ExternalEventPublisher publisher;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("Should publish event to Kafka topic")
    void shouldPublishEventToKafkaTopic() throws Exception {
        // given
        ExternalTestEvent event = ExternalTestEvent.create("Test payload for Kafka");
        
        // Create consumer to verify message was sent
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            org.apache.kafka.common.serialization.StringDeserializer.class);
        
        ConsumerFactory<String, String> consumerFactory = 
            new DefaultKafkaConsumerFactory<>(props);
        var consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAllEmbeddedTopics(consumer);

        // when
        publisher.publish(event);

        // then
        ConsumerRecord<String, String> record = 
            KafkaTestUtils.getSingleRecord(consumer, "test-events", Duration.ofSeconds(5));
        assertNotNull(record);
        assertNotNull(record.value());
        assertTrue(record.value().contains("Test payload for Kafka"));
        assertTrue(record.value().contains("ExternalTestEvent"));
        
        consumer.close();
    }

    @Test
    @DisplayName("Should return EXTERNAL event bus")
    void shouldReturnExternalEventBus() {
        // when
        EventBus eventBus = publisher.eventBus();

        // then
        assertEquals(EventBus.EXTERNAL, eventBus);
    }

    @Test
    @DisplayName("Should serialize event to JSON before sending to Kafka")
    void shouldSerializeEventToJsonBeforeSendingToKafka() throws Exception {
        // given
        ExternalTestEvent event = new ExternalTestEvent(
            "test-id-123", 
            "Serialized content", 
            java.time.LocalDateTime.now()
        );
        
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            org.apache.kafka.common.serialization.StringDeserializer.class);
        
        ConsumerFactory<String, String> consumerFactory = 
            new DefaultKafkaConsumerFactory<>(props);
        var consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromEmbeddedTopics(consumer, "test-events");

        // when
        publisher.publish(event);

        // then
        ConsumerRecord<String, String> record = 
            KafkaTestUtils.getSingleRecord(consumer, "test-events", Duration.ofSeconds(5));
        assertNotNull(record);
        String json = record.value();
        assertTrue(json.contains("test-id-123"));
        assertTrue(json.contains("Serialized content"));
        
        consumer.close();
    }
}
