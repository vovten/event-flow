package com.github.vovten.eventflow.autoconfig.transport.dispatcher;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import com.github.vovten.eventflow.transport.DispatcherTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for in-memory dispatcher transport factory.
 */
class InMemoryDispatcherTransportFactoryTest {

    @Test
    @DisplayName("InMemoryDispatcherTransportFactory should have correct type")
    void inMemoryDispatcherTransportFactoryShouldHaveCorrectType() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryDispatcherTransportFactory factory = new InMemoryDispatcherTransportFactory(queueProvider);

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("in-memory");
    }

    @Test
    @DisplayName("InMemoryDispatcherTransportFactory should create dispatcher transport")
    void inMemoryDispatcherTransportFactoryShouldCreateDispatcherTransport() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryDispatcherTransportFactory factory = new InMemoryDispatcherTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when
        DispatcherTransport transport = factory.createDispatcher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("InMemoryDispatcherTransportFactory should validate config without error")
    void inMemoryDispatcherTransportFactoryShouldValidateConfigWithoutError() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryDispatcherTransportFactory factory = new InMemoryDispatcherTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when & then - should not throw any exception
        factory.validate(config);
    }
}
