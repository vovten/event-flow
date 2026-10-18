package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.AbstractKafkaOutTransport;
import io.github.vovten.eventflow.transport.outgoing.EventKeyResolver;
import io.github.vovten.eventflow.transport.outgoing.KafkaOutTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.BeanFactory;

import java.lang.reflect.Field;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Unit tests for Kafka publisher transport factory.
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
class KafkaOutTransportFactoryTest {

    @Mock
    private BeanFactory beanFactory;

    private final EventSerializerFactory serializerFactory = new EventSerializerFactory();

    private KafkaOutTransportFactory factory;

    @BeforeEach
    void setUp() {
        factory = new KafkaOutTransportFactory(serializerFactory, beanFactory);
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should have correct name")
    void kafkaPublisherTransportFactoryShouldHaveCorrectName() {
        // when
        String name = factory.getName();

        // then
        assertThat(name).isEqualTo("kafka");
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should create publisher transport")
    void kafkaPublisherTransportFactoryShouldCreatePublisherTransport() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();

        // when
        OutTransport transport = factory.createPublisher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should throw exception when servers is missing")
    void kafkaPublisherTransportFactoryShouldThrowExceptionWhenServersIsMissing() {
        // given
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setTopics("test-topic");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires 'servers' configuration");
    }

    @Test
    @DisplayName("KafkaPublisherTransportFactory should throw exception when topics is missing")
    void kafkaPublisherTransportFactoryShouldThrowExceptionWhenTopicsIsMissing() {
        // given
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setServers("localhost:9092");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka transport requires topics configuration");
    }

    @Test
    @DisplayName("Should resolve no key strategy by default")
    void shouldResolveNoneKeyStrategyByDefault() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();
        // key defaults to "none"

        // when
        KafkaOutTransport transport = (KafkaOutTransport) factory.createPublisher(config);

        // then
        assertThat(resolveKey(transport, Envelope.of("payload"))).isNull();
    }

    @Test
    @DisplayName("Should resolve process-id key strategy")
    void shouldResolveProcessIdKeyStrategy() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();
        config.setKey("process-id");
        UUID processId = UUID.randomUUID();
        Envelope<String> envelope = Envelope.of("payload", processId);

        // when
        KafkaOutTransport transport = (KafkaOutTransport) factory.createPublisher(config);

        // then
        assertThat(resolveKey(transport, envelope)).isEqualTo(processId.toString());
        assertThat(resolveKey(transport, Envelope.of("payload"))).isNull();
    }

    @Test
    @DisplayName("Should resolve event-type key strategy")
    void shouldResolveEventTypeKeyStrategy() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();
        config.setKey("event-type");
        Event event = Envelope.of("payload");

        // when
        KafkaOutTransport transport = (KafkaOutTransport) factory.createPublisher(config);

        // then
        assertThat(resolveKey(transport, event)).isEqualTo(event.type().getName());
    }

    @Test
    @DisplayName("Should resolve metadata key strategy")
    void shouldResolveMetadataKeyStrategy() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();
        config.setKey("metadata:orderId");
        Envelope<String> envelope = Envelope.of("payload").withAdditionalMetadata("orderId", "order-123");

        // when
        KafkaOutTransport transport = (KafkaOutTransport) factory.createPublisher(config);

        // then
        assertThat(resolveKey(transport, envelope)).isEqualTo("order-123");
        assertThat(resolveKey(transport, Envelope.of("payload"))).isNull();
    }

    @Test
    @DisplayName("Should resolve bean key strategy from bean factory")
    void shouldResolveBeanKeyStrategy() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();
        config.setKey("bean:orderKeyResolver");
        // A custom resolver bean: extract orderId from the payload
        EventKeyResolver custom = event -> {
            if (event instanceof Envelope<?> env && env.payload() instanceof OrderCreated order) {
                return order.orderId();
            }
            return null;
        };
        when(beanFactory.getBean("orderKeyResolver", EventKeyResolver.class)).thenReturn(custom);

        // when
        KafkaOutTransport transport = (KafkaOutTransport) factory.createPublisher(config);

        // then
        assertThat(resolveKey(transport, Envelope.of(new OrderCreated("order-123"))))
                .isEqualTo("order-123");
        // events without an order id fall back to round-robin
        assertThat(resolveKey(transport, Envelope.of("plain-payload"))).isNull();
    }

    @Test
    @DisplayName("Should throw exception for unknown key strategy")
    void shouldThrowExceptionForUnknownKeyStrategy() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();
        config.setKey("unknown-strategy");

        // when & then
        assertThatThrownBy(() -> factory.createPublisher(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown Kafka 'key' strategy 'unknown-strategy'");
    }

    @Test
    @DisplayName("Should throw exception for metadata strategy without name")
    void shouldThrowExceptionForMetadataStrategyWithoutName() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();
        config.setKey("metadata:");

        // when & then
        assertThatThrownBy(() -> factory.createPublisher(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a metadata name");
    }

    @Test
    @DisplayName("Should throw exception for bean strategy without name")
    void shouldThrowExceptionForBeanStrategyWithoutName() {
        // given
        EventFlowProperties.TransportConfig config = transportConfig();
        config.setKey("bean:");

        // when & then
        assertThatThrownBy(() -> factory.createPublisher(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a bean name");
    }

    private EventFlowProperties.TransportConfig transportConfig() {
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("kafka-publisher");
        config.setServers("localhost:9092");
        config.setTopics("test-topic");
        return config;
    }

    private String resolveKey(KafkaOutTransport transport, Event event) {
        try {
            Field field = AbstractKafkaOutTransport.class.getDeclaredField("keyResolver");
            field.setAccessible(true);
            EventKeyResolver resolver = (EventKeyResolver) field.get(transport);
            return resolver.resolveKey(event);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to read keyResolver field", e);
        }
    }

    private record OrderCreated(String orderId) {
    }
}