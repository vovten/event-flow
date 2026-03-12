package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for Kafka transport factories.
 */
class KafkaTransportFactoryTest {

    @Test
    @DisplayName("KafkaIncomingTransportFactory should have correct type")
    void kafkaIncomingTransportFactoryShouldHaveCorrectType() {
        // given
        KafkaIncomingTransportFactory factory = new KafkaIncomingTransportFactory();

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("kafka");
    }

    @Test
    @DisplayName("KafkaIncomingTransportFactory should create incoming transport")
    void kafkaIncomingTransportFactoryShouldCreateIncomingTransport() {
        // given
        KafkaIncomingTransportFactory factory = new KafkaIncomingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("kafka-incoming");
        config.setType("kafka");
        config.setBootstrapServers("localhost:9092");
        config.setTopic("test-topic");
        config.setConsumerGroup("test-group");

        // when
        IncomingEventTransport transport = factory.createIncoming(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("KafkaIncomingTransportFactory should throw exception when bootstrapServers is missing")
    void kafkaIncomingTransportFactoryShouldThrowExceptionWhenBootstrapServersIsMissing() {
        // given
        KafkaIncomingTransportFactory factory = new KafkaIncomingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopic("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires bootstrap-servers configuration");
    }

    @Test
    @DisplayName("KafkaIncomingTransportFactory should throw exception when topic is missing")
    void kafkaIncomingTransportFactoryShouldThrowExceptionWhenTopicIsMissing() {
        // given
        KafkaIncomingTransportFactory factory = new KafkaIncomingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setBootstrapServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires topic configuration");
    }

    @Test
    @DisplayName("KafkaOutgoingTransportFactory should have correct type")
    void kafkaOutgoingTransportFactoryShouldHaveCorrectType() {
        // given
        KafkaOutgoingTransportFactory factory = new KafkaOutgoingTransportFactory();

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("kafka");
    }

    @Test
    @DisplayName("KafkaOutgoingTransportFactory should create outgoing transport")
    void kafkaOutgoingTransportFactoryShouldCreateOutgoingTransport() {
        // given
        KafkaOutgoingTransportFactory factory = new KafkaOutgoingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("kafka-outgoing");
        config.setType("kafka");
        config.setBootstrapServers("localhost:9092");
        config.setTopic("test-topic");

        // when
        OutgoingEventTransport transport = factory.createOutgoing(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("KafkaOutgoingTransportFactory should throw exception when bootstrapServers is missing")
    void kafkaOutgoingTransportFactoryShouldThrowExceptionWhenBootstrapServersIsMissing() {
        // given
        KafkaOutgoingTransportFactory factory = new KafkaOutgoingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopic("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires bootstrap-servers configuration");
    }

    @Test
    @DisplayName("KafkaOutgoingTransportFactory should throw exception when topic is missing")
    void kafkaOutgoingTransportFactoryShouldThrowExceptionWhenTopicIsMissing() {
        // given
        KafkaOutgoingTransportFactory factory = new KafkaOutgoingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setBootstrapServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires topic configuration");
    }
}
