package com.github.vovten.eventflow.autoconfig.transport.publisher;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import com.github.vovten.eventflow.transport.PublisherTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for in-memory publisher transport factory.
 */
class InMemoryPublisherTransportFactoryTest {

    @Test
    @DisplayName("InMemoryPublisherTransportFactory should have correct type")
    void inMemoryPublisherTransportFactoryShouldHaveCorrectType() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryPublisherTransportFactory factory = new InMemoryPublisherTransportFactory(queueProvider);

        // when
        String type = factory.getName();

        // then
        assertThat(type).isEqualTo("in-memory");
    }

    @Test
    @DisplayName("InMemoryPublisherTransportFactory should create publisher transport")
    void inMemoryPublisherTransportFactoryShouldCreatePublisherTransport() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryPublisherTransportFactory factory = new InMemoryPublisherTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when
        PublisherTransport transport = factory.createPublisher(config);

        // then
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("InMemoryPublisherTransportFactory should validate config without error")
    void inMemoryPublisherTransportFactoryShouldValidateConfigWithoutError() {
        // given
        DefaultQueueProvider queueProvider = new DefaultQueueProvider(1000);
        InMemoryPublisherTransportFactory factory = new InMemoryPublisherTransportFactory(queueProvider);

        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("test-queue");

        // when & then - should not throw any exception
        factory.validate(config);
    }
}
