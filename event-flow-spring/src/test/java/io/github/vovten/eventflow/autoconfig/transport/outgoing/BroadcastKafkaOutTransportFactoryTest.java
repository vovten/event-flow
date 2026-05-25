package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link BroadcastKafkaOutTransportFactory}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@DisplayName("BroadcastKafkaPublisherTransportFactory Tests")
class BroadcastKafkaOutTransportFactoryTest {

    private final EventSerializerFactory serializerFactory = new EventSerializerFactory();

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should have correct name")
    void broadcastKafkaPublisherTransportFactoryShouldHaveCorrectName() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory(serializerFactory);

        // when
        String name = factory.getName();

        // then
        assertThat(name).isEqualTo("broadcast-kafka");
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should create publisher transport")
    void broadcastKafkaPublisherTransportFactoryShouldCreatePublisherTransport() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("broadcast-kafka-publisher");
        config.setServers("localhost:9092");
        config.setTopics("test-topic");

        // when
        OutTransport transport = factory.createPublisher(config);

        // then
        assertThat(transport).isNotNull();
        assertThat(transport).isInstanceOf(BroadcastKafkaOutTransport.class);
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should throw exception when servers is missing")
    void broadcastKafkaPublisherTransportFactoryShouldThrowExceptionWhenServersIsMissing() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopics("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broadcast Kafka transport requires 'servers' configuration");
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should throw exception when topics is missing")
    void broadcastKafkaPublisherTransportFactoryShouldThrowExceptionWhenTopicsIsMissing() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Broadcast Kafka transport requires topics configuration");
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should validate successfully with all required config")
    void broadcastKafkaPublisherTransportFactoryShouldValidateSuccessfullyWithAllRequiredConfig() {
        // given
        BroadcastKafkaOutTransportFactory factory = new BroadcastKafkaOutTransportFactory(serializerFactory);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("broadcast-kafka");
        config.setServers("localhost:9092");
        config.setTopics("test-topic");

        // when & then - should not throw any exception
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> factory.validate(config));
    }
}
