package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import io.github.vovten.eventflow.transport.OutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for local-queue publisher transport factory.
 * @since 1.0.0
 */
class LocalQueueOutTransportFactoryTest {

    @Test
    @DisplayName("LocalQueuePublisherTransportFactory should have correct type")
    void localQueuePublisherTransportFactoryShouldHaveCorrectType() {
        // given
        DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
        LocalQueueOutTransportFactory factory = new LocalQueueOutTransportFactory(queueProvider);

        // when
        String type = factory.getName();

        // then
        assertThat(type).isEqualTo("local-queue");
    }

    @Test
    @DisplayName("LocalQueuePublisherTransportFactory should create publisher transport")
    void localQueuePublisherTransportFactoryShouldCreatePublisherTransport() {
        // given
        DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
        LocalQueueOutTransportFactory factory = new LocalQueueOutTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when
        OutTransport transport = factory.createPublisher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("LocalQueuePublisherTransportFactory should validate config without error")
    void localQueuePublisherTransportFactoryShouldValidateConfigWithoutError() {
        // given
        DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
        LocalQueueOutTransportFactory factory = new LocalQueueOutTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when & then - should not throw any exception
        factory.validate(config);
    }
}
