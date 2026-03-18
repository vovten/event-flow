package com.github.vovten.eventflow.autoconfig.transport.publisher;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.PublisherTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for Kafka publisher transport factory.
 */
class KafkaPublisherTransportFactoryTest {

    @Test
    @DisplayName("KafkaPublisherTransportFactory should have correct name")
    void kafkaPublisherTransportFactoryShouldHaveCorrectName() {
        // given
        KafkaPublisherTransportFactory factory = new KafkaPublisherTransportFactory();

        // when
        String name = factory.getName();

        // then
        assertThat(name).isEqualTo("kafka");
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should create publisher transport")
    void kafkaPublisherTransportFactoryShouldCreatePublisherTransport() {
        // given
        KafkaPublisherTransportFactory factory = new KafkaPublisherTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("kafka-publisher");
        config.setBootstrapServers("localhost:9092");
        config.setTopic("test-topic");

        // when
        PublisherTransport transport = factory.createPublisher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should throw exception when bootstrapServers is missing")
    void kafkaPublisherTransportFactoryShouldThrowExceptionWhenBootstrapServersIsMissing() {
        // given
        KafkaPublisherTransportFactory factory = new KafkaPublisherTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopic("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires bootstrap-servers configuration");
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should throw exception when topic is missing")
    void kafkaPublisherTransportFactoryShouldThrowExceptionWhenTopicIsMissing() {
        // given
        KafkaPublisherTransportFactory factory = new KafkaPublisherTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setBootstrapServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires topic configuration");
    }
}
