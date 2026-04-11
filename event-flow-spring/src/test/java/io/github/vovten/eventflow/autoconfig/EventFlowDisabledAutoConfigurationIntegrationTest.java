package io.github.vovten.eventflow.autoconfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link EventFlowDisabledAutoConfiguration}.
 * Verifies that the disabled configuration loads correctly.
 */
@SpringBootTest(classes = EventFlowDisabledAutoConfigurationIntegrationTest.TestConfig.class)
@TestPropertySource(properties = {
    "event-flow.enabled=false",
    "event-flow.publisher.enabled=false",
    "event-flow.dispatcher.enabled=false"
})
class EventFlowDisabledAutoConfigurationIntegrationTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("Should load EventFlowDisabledAutoConfiguration when event-flow is disabled")
    void shouldLoadConfigurationWhenDisabled() {
        // then
        assertThat(context.getBeanNamesForType(EventFlowDisabledAutoConfiguration.EventFlowDisabledLogger.class)).hasSize(1);
    }

    @Test
    @DisplayName("Should contain eventFlowDisabledLogger bean when disabled")
    void shouldContainEventFlowDisabledLoggerBean() {
        // then
        assertThat(context.getBean(EventFlowDisabledAutoConfiguration.EventFlowDisabledLogger.class)).isNotNull();
    }

    @Test
    @DisplayName("Should not contain EventPublisher bean when disabled")
    void shouldNotContainEventPublisherBean() {
        // then
        assertThat(context.getBeanNamesForType(io.github.vovten.eventflow.publisher.EventPublisher.class)).isEmpty();
    }

    @Test
    @DisplayName("Should not contain EventDispatcher bean when disabled")
    void shouldNotContainEventDispatcherBean() {
        // then
        assertThat(context.getBeanNamesForType(io.github.vovten.eventflow.dispatcher.EventDispatcher.class)).isEmpty();
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestConfig {
    }
}
