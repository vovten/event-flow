package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for in-memory transport factories.
 */
class InMemoryTransportFactoryTest {

    @Test
    @DisplayName("InMemoryIncomingTransportFactory should have correct type")
    void inMemoryIncomingTransportFactoryShouldHaveCorrectType() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryIncomingTransportFactory factory = new InMemoryIncomingTransportFactory(queueProvider);

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("in-memory");
    }

    @Test
    @DisplayName("InMemoryIncomingTransportFactory should create incoming transport")
    void inMemoryIncomingTransportFactoryShouldCreateIncomingTransport() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryIncomingTransportFactory factory = new InMemoryIncomingTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when
        IncomingEventTransport transport = factory.createIncoming(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("InMemoryIncomingTransportFactory should validate config without error")
    void inMemoryIncomingTransportFactoryShouldValidateConfigWithoutError() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryIncomingTransportFactory factory = new InMemoryIncomingTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when & then - should not throw any exception
        factory.validate(config);
    }

    @Test
    @DisplayName("InMemoryOutgoingTransportFactory should have correct type")
    void inMemoryOutgoingTransportFactoryShouldHaveCorrectType() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryOutgoingTransportFactory factory = new InMemoryOutgoingTransportFactory(queueProvider);

        // when
        String type = factory.getName();

        // then
        assertThat(type).isEqualTo("in-memory");
    }

    @Test
    @DisplayName("InMemoryOutgoingTransportFactory should create outgoing transport")
    void inMemoryOutgoingTransportFactoryShouldCreateOutgoingTransport() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryOutgoingTransportFactory factory = new InMemoryOutgoingTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when
        OutgoingEventTransport transport = factory.createOutgoing(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("InMemoryOutgoingTransportFactory should validate config without error")
    void inMemoryOutgoingTransportFactoryShouldValidateConfigWithoutError() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryOutgoingTransportFactory factory = new InMemoryOutgoingTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when & then - should not throw any exception
        factory.validate(config);
    }
}
