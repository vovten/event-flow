package io.github.vovten.eventflow.autoconfig.transport.incoming;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for Kafka dispatcher transport factory.
 * @since 1.0.0
 */
class KafkaInTransportFactoryTest {

    private final EventSerializerFactory serializerFactory = new EventSerializerFactory();

    @Test
    @DisplayName("KafkaDispatcherTransportFactory should have correct type")
    void kafkaDispatcherTransportFactoryShouldHaveCorrectType() {
        // given
        KafkaInTransportFactory factory = new KafkaInTransportFactory(serializerFactory);

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("kafka");
    }

    @Test
    @DisplayName("KafkaDispatcherTransportFactory should create dispatcher transport")
    void kafkaDispatcherTransportFactoryShouldCreateDispatcherTransport() {
        // given
        KafkaInTransportFactory factory = new KafkaInTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("kafka-dispatcher");
        config.setServers("localhost:9092");
        config.setTopics("test-topic");
        config.setConsumerGroup("test-group");

        // when
        InTransport transport = factory.createDispatcher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("KafkaDispatcherTransportFactory should throw exception when servers is missing")
    void kafkaDispatcherTransportFactoryShouldThrowExceptionWhenServersIsMissing() {
        // given
        KafkaInTransportFactory factory = new KafkaInTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopics("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires 'servers' configuration");
    }

    @Test
    @DisplayName("KafkaDispatcherTransportFactory should throw exception when topics is missing")
    void kafkaDispatcherTransportFactoryShouldThrowExceptionWhenTopicsIsMissing() {
        // given
        KafkaInTransportFactory factory = new KafkaInTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires topics configuration");
    }
}
