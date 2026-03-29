package com.github.vovten.eventflow.autoconfig.transport.incoming;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import com.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for in-memory dispatcher transport factory.
 */
class LocalQueueInTransportFactoryTest {

    @Test
    @DisplayName("InMemoryDispatcherTransportFactory should have correct type")
    void inMemoryDispatcherTransportFactoryShouldHaveCorrectType() {
        // given
        DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
        LocalQueueInTransportFactory factory = new LocalQueueInTransportFactory(queueProvider);

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("in-memory");
    }

    @Test
    @DisplayName("InMemoryDispatcherTransportFactory should create dispatcher transport")
    void inMemoryDispatcherTransportFactoryShouldCreateDispatcherTransport() {
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
    @DisplayName("InMemoryDispatcherTransportFactory should validate config without error")
    void inMemoryDispatcherTransportFactoryShouldValidateConfigWithoutError() {
        // given
        DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
        LocalQueueInTransportFactory factory = new LocalQueueInTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when & then - should not throw any exception
        factory.validate(config);
    }
}
