package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import com.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutgoingEventTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link BroadcastKafkaOutgoingTransportFactory}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 */
@DisplayName("BroadcastKafkaOutgoingTransportFactory Tests")
class BroadcastKafkaOutgoingTransportFactoryTest {

    @Test
    @DisplayName("BroadcastKafkaOutgoingTransportFactory should have correct name")
    void broadcastKafkaOutgoingTransportFactoryShouldHaveCorrectName() {
        // given
        BroadcastKafkaOutgoingTransportFactory factory = new BroadcastKafkaOutgoingTransportFactory();

        // when
        String name = factory.getName();

        // then
        assertThat(name).isEqualTo("broadcast-kafka");
    }

    @Test
    @DisplayName("BroadcastKafkaOutgoingTransportFactory should create outgoing transport")
    void broadcastKafkaOutgoingTransportFactoryShouldCreateOutgoingTransport() {
        // given
        BroadcastKafkaOutgoingTransportFactory factory = new BroadcastKafkaOutgoingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("broadcast-kafka-outgoing");
        config.setBootstrapServers("localhost:9092");
        config.setTopic("test-topic");

        // when
        OutgoingEventTransport transport = factory.createOutgoing(config);

        // then
        assertThat(transport).isNotNull();
        assertThat(transport).isInstanceOf(BroadcastKafkaOutgoingEventTransport.class);
    }

    @Test
    @DisplayName("BroadcastKafkaOutgoingTransportFactory should throw exception when bootstrapServers is missing")
    void broadcastKafkaOutgoingTransportFactoryShouldThrowExceptionWhenBootstrapServersIsMissing() {
        // given
        BroadcastKafkaOutgoingTransportFactory factory = new BroadcastKafkaOutgoingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopic("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broadcast Kafka transport requires bootstrap-servers configuration");
    }

    @Test
    @DisplayName("BroadcastKafkaOutgoingTransportFactory should throw exception when topic is missing")
    void broadcastKafkaOutgoingTransportFactoryShouldThrowExceptionWhenTopicIsMissing() {
        // given
        BroadcastKafkaOutgoingTransportFactory factory = new BroadcastKafkaOutgoingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setBootstrapServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broadcast Kafka transport requires topic configuration");
    }

    @Test
    @DisplayName("BroadcastKafkaOutgoingTransportFactory should validate successfully with all required config")
    void broadcastKafkaOutgoingTransportFactoryShouldValidateSuccessfullyWithAllRequiredConfig() {
        // given
        BroadcastKafkaOutgoingTransportFactory factory = new BroadcastKafkaOutgoingTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("broadcast-kafka");
        config.setBootstrapServers("localhost:9092");
        config.setTopic("test-topic");

        // when & then - should not throw any exception
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> factory.validate(config));
    }
}
