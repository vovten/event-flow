package com.github.vovten.eventflow.autoconfig.transport.outgoing;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link BroadcastKafkaOutTransportFactory}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 */
@DisplayName("BroadcastKafkaPublisherTransportFactory Tests")
class BroadcastKafkaOutTransportFactoryTest {

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should have correct name")
    void broadcastKafkaPublisherTransportFactoryShouldHaveCorrectName() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory();

        // when
        String name = factory.getName();

        // then
        assertThat(name).isEqualTo("broadcast-kafka");
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should create publisher transport")
    void broadcastKafkaPublisherTransportFactoryShouldCreatePublisherTransport() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("broadcast-kafka-publisher");
        config.setBootstrapServers("localhost:9092");
        config.setTopic("test-topic");

        // when
        OutTransport transport = factory.createPublisher(config);

        // then
        assertThat(transport).isNotNull();
        assertThat(transport).isInstanceOf(BroadcastKafkaOutTransport.class);
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should throw exception when bootstrapServers is missing")
    void broadcastKafkaPublisherTransportFactoryShouldThrowExceptionWhenBootstrapServersIsMissing() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopic("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broadcast Kafka transport requires bootstrap-servers configuration");
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should throw exception when topic is missing")
    void broadcastKafkaPublisherTransportFactoryShouldThrowExceptionWhenTopicIsMissing() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setBootstrapServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broadcast Kafka transport requires topic configuration");
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should validate successfully with all required config")
    void broadcastKafkaPublisherTransportFactoryShouldValidateSuccessfullyWithAllRequiredConfig() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("broadcast-kafka");
        config.setBootstrapServers("localhost:9092");
        config.setTopic("test-topic");

        // when & then - should not throw any exception
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> factory.validate(config));
    }
}
