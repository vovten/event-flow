package com.github.vovten.eventflow.autoconfig.transport.outgoing;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.serialization.EventSerializerFactory;
import com.github.vovten.eventflow.transport.OutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for Kafka publisher transport factory.
 */
class KafkaOutTransportFactoryTest {

    private final EventSerializerFactory serializerFactory = new EventSerializerFactory();

    @Test
    @DisplayName("KafkaPublisherTransportFactory should have correct name")
    void kafkaPublisherTransportFactoryShouldHaveCorrectName() {
        // given
        KafkaOutTransportFactory factory = new KafkaOutTransportFactory(serializerFactory);

        // when
        String name = factory.getName();

        // then
        assertThat(name).isEqualTo("kafka");
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should create publisher transport")
    void kafkaPublisherTransportFactoryShouldCreatePublisherTransport() {
        // given
        KafkaOutTransportFactory factory = new KafkaOutTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("kafka-publisher");
        config.setServers("localhost:9092");
        config.setTopic("test-topic");

        // when
        OutTransport transport = factory.createPublisher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should throw exception when servers is missing")
    void kafkaPublisherTransportFactoryShouldThrowExceptionWhenServersIsMissing() {
        // given
        KafkaOutTransportFactory factory = new KafkaOutTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopic("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires 'servers' configuration");
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should throw exception when topic is missing")
    void kafkaPublisherTransportFactoryShouldThrowExceptionWhenTopicIsMissing() {
        // given
        KafkaOutTransportFactory factory = new KafkaOutTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires topic configuration");
    }
}
