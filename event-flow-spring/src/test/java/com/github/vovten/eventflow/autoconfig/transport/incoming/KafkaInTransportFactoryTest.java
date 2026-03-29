package com.github.vovten.eventflow.autoconfig.transport.incoming;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for Kafka dispatcher transport factory.
 */
class KafkaInTransportFactoryTest {

    @Test
    @DisplayName("KafkaDispatcherTransportFactory should have correct type")
    void kafkaDispatcherTransportFactoryShouldHaveCorrectType() {
        // given
        KafkaInTransportFactory factory = new KafkaInTransportFactory();

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("kafka");
    }

    @Test
    @DisplayName("KafkaDispatcherTransportFactory should create dispatcher transport")
    void kafkaDispatcherTransportFactoryShouldCreateDispatcherTransport() {
        // given
        KafkaInTransportFactory factory = new KafkaInTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("kafka-dispatcher");
        config.setBootstrapServers("localhost:9092");
        config.setTopic("test-topic");
        config.setConsumerGroup("test-group");

        // when
        InTransport transport = factory.createDispatcher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("KafkaDispatcherTransportFactory should throw exception when bootstrapServers is missing")
    void kafkaDispatcherTransportFactoryShouldThrowExceptionWhenBootstrapServersIsMissing() {
        // given
        KafkaInTransportFactory factory = new KafkaInTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopic("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires bootstrap-servers configuration");
    }

    @Test
    @DisplayName("KafkaDispatcherTransportFactory should throw exception when topic is missing")
    void kafkaDispatcherTransportFactoryShouldThrowExceptionWhenTopicIsMissing() {
        // given
        KafkaInTransportFactory factory = new KafkaInTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setBootstrapServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires topic configuration");
    }
}
