package io.github.vovten.eventflow.autoconfig;

import io.github.vovten.eventflow.registry.CompositeEventHandlerRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link EventFlowAutoConfiguration}.
 * Verifies that auto-configuration properly loads all beans into Spring context.
 * @since 1.0.0
 */
@SpringBootTest(classes = EventFlowAutoConfiguration.class)
@TestPropertySource(properties = {
    "event-flow.enabled=true",
    "event-flow.dispatcher.listener-packages=io.github.vovten.eventflow",
    "event-flow.publisher.enabled=true",
    "event-flow.publisher.channels[0].name=internal",
    "event-flow.publisher.channels[0].transports[0].name=local-queue",
    "event-flow.publisher.channels[0].transports[0].capacity=1000",
    "event-flow.dispatcher.enabled=true",
    "event-flow.dispatcher.transports[0].name=local-queue",
    "event-flow.dispatcher.transports[0].capacity=1000",
    "event-flow.dispatcher.thread-pool.core-size=2",
    "event-flow.dispatcher.thread-pool.max-size=4",
    "event-flow.dispatcher.thread-pool.queue-capacity=100"
})
class EventFlowAutoConfigurationTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("Should load EventFlow auto-configuration with all required beans")
    void shouldLoadAutoConfigurationWithAllBeans() {
        assertThat(context.containsBean("springEventListenerRegistry")).isTrue();
        assertThat(context.containsBean("springEventSubscriberRegistry")).isTrue();
        assertThat(context.containsBean("eventHandlerRegistry")).isTrue();
        CompositeEventHandlerRegistry compositeRegistry =
                (CompositeEventHandlerRegistry) context.getBean("eventHandlerRegistry");
        assertThat(compositeRegistry).isNotNull();
        assertThat(context.containsBean("eventPublisher")).isTrue();
        assertThat(context.containsBean("eventDispatcher")).isTrue();
    }
}
