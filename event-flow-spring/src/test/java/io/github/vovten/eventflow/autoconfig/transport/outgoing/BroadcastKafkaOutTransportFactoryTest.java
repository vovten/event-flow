package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.AbstractKafkaOutTransport;
import io.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutTransport;
import io.github.vovten.eventflow.transport.outgoing.EventKeyResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    private BroadcastKafkaOutTransportFactory newFactory() {
        return new BroadcastKafkaOutTransportFactory(serializerFactory, null);
    }
    /**
     * {@code bean:} key strategy is not covered here: it requires a BeanFactory mock,
     * mirroring {@link KafkaOutTransportFactoryTest#shouldResolveBeanKeyStrategy}.
     */

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should have correct name")
    void broadcastKafkaPublisherTransportFactoryShouldHaveCorrectName() {
        // given
        BroadcastKafkaOutTransportFactory factory = newFactory();

        // when
        String name = factory.getName();

        // then
        assertThat(name).isEqualTo("broadcast-kafka");
    }

    @Test
    @DisplayName("BroadcastKafkaPublisherTransportFactory should create publisher transport")
    void broadcastKafkaPublisherTransportFactoryShouldCreatePublisherTransport() {
        // given
        BroadcastKafkaOutTransportFactory factory = newFactory();

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
        BroadcastKafkaOutTransportFactory factory = newFactory();

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
        BroadcastKafkaOutTransportFactory factory = newFactory();

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
        BroadcastKafkaOutTransportFactory factory = newFactory();

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("broadcast-kafka");
        config.setServers("localhost:9092");
        config.setTopics("test-topic");

        // when & then - should not throw any exception
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> factory.validate(config));
    }

    @Test
    @DisplayName("Should apply metadata key strategy to broadcast transport")
    void shouldApplyMetadataKeyStrategyToBroadcastTransport() {
        // given
        BroadcastKafkaOutTransportFactory factory = newFactory();
        EventFlowProperties.TransportConfig config = broadcastConfig();
        config.setKey("metadata:orderId");

        // when
        BroadcastKafkaOutTransport transport = (BroadcastKafkaOutTransport) factory.createPublisher(config);

        // then
        Envelope<String> envelope = Envelope.of("payload").withAdditionalMetadata("orderId", "order-123");
        assertThat(resolveKey(transport, envelope)).isEqualTo("order-123");
    }

    @Test
    @DisplayName("Should fall back to null key when metadata entry is absent on broadcast transport")
    void shouldFallBackToNullKeyWhenMetadataEntryIsAbsent() {
        // given
        BroadcastKafkaOutTransportFactory factory = newFactory();
        EventFlowProperties.TransportConfig config = broadcastConfig();
        config.setKey("metadata:orderId");

        // when
        BroadcastKafkaOutTransport transport = (BroadcastKafkaOutTransport) factory.createPublisher(config);

        // then
        Envelope<String> envelope = Envelope.of("payload");
        assertThat(resolveKey(transport, envelope)).isNull();
    }

    @Test
    @DisplayName("Should default broadcast transport to none key strategy like regular kafka transport")
    void shouldDefaultBroadcastTransportToNoneKeyStrategy() {
        // given
        BroadcastKafkaOutTransportFactory factory = newFactory();
        EventFlowProperties.TransportConfig config = broadcastConfig();
        // key defaults to "none" — consistent with KafkaOutTransportFactory

        // when
        BroadcastKafkaOutTransport transport = (BroadcastKafkaOutTransport) factory.createPublisher(config);

        // then
        assertThat(resolveKey(transport, Envelope.of("payload"))).isNull();
    }

    @Test
    @DisplayName("Should reject unknown key strategy on broadcast transport during validation")
    void shouldRejectUnknownKeyStrategyOnBroadcastTransportDuringValidation() {
        // given
        BroadcastKafkaOutTransportFactory factory = newFactory();
        EventFlowProperties.TransportConfig config = broadcastConfig();
        config.setKey("partition-key");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown Kafka 'key' strategy 'partition-key'");
    }

    @Test
    @DisplayName("Should accept known key strategies on broadcast transport during validation")
    void shouldAcceptKnownKeyStrategiesOnBroadcastTransportDuringValidation() {
        // given
        BroadcastKafkaOutTransportFactory factory = newFactory();
        EventFlowProperties.TransportConfig config = broadcastConfig();

        // when & then: no exception for any supported strategy
        for (String key : new String[]{null, "", "none", "process-id", "event-type", "metadata:orderId"}) {
            config.setKey(key);
            assertThatCode(() -> factory.validate(config)).doesNotThrowAnyException();
        }
    }

    private EventFlowProperties.TransportConfig broadcastConfig() {
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("broadcast-kafka");
        config.setServers("localhost:9092");
        config.setTopics("test-topic");
        return config;
    }

    private String resolveKey(BroadcastKafkaOutTransport transport, Event event) {
        try {
            Field field = AbstractKafkaOutTransport.class.getDeclaredField("keyResolver");
            field.setAccessible(true);
            EventKeyResolver resolver = (EventKeyResolver) field.get(transport);
            return resolver.resolveKey(event);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to read keyResolver field", e);
        }
    }
}
