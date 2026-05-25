package io.github.vovten.eventflow.autoconfig.transport.incoming;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import io.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for local-queue dispatcher transport factory.
 * @since 1.0.0
 */
class LocalQueueInTransportFactoryTest {

    @Test
    @DisplayName("LocalQueueDispatcherTransportFactory should have correct type")
    void localQueueDispatcherTransportFactoryShouldHaveCorrectType() {
        // given
        DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
        LocalQueueInTransportFactory factory = new LocalQueueInTransportFactory(queueProvider);

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("local-queue");
    }

    @Test
    @DisplayName("LocalQueueDispatcherTransportFactory should create dispatcher transport")
    void localQueueDispatcherTransportFactoryShouldCreateDispatcherTransport() {
        // given
        DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
        LocalQueueInTransportFactory factory = new LocalQueueInTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when
        InTransport transport = factory.createDispatcher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("LocalQueueDispatcherTransportFactory should validate config without error")
    void localQueueDispatcherTransportFactoryShouldValidateConfigWithoutError() {
        // given
        DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
        LocalQueueInTransportFactory factory = new LocalQueueInTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when & then - should not throw any exception
        factory.validate(config);
    }
}
