package com.github.vovten.eventflow.autoconfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link EventFlowDisabledAutoConfiguration}.
 * Verifies that the disabled configuration loads and logs the expected message.
 */
@SpringBootTest(classes = EventFlowDisabledAutoConfigurationIntegrationTest.TestConfig.class)
@TestPropertySource(properties = {
    "event-flow.enabled=false"
})
@ExtendWith(OutputCaptureExtension.class)
class EventFlowDisabledAutoConfigurationIntegrationTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("Should load EventFlowDisabledAutoConfiguration and log disabled message when event-flow is disabled")
    void shouldLoadConfigurationAndLogDisabledMessage(CapturedOutput output) {
        // then
        assertThat(output).contains("Event Flow is disabled");
        assertThat(output).contains("To enable Event Flow auto-configuration");
        assertThat(output).contains("event-flow:");
        assertThat(output).contains("enabled: true");
        assertThat(output).contains("scan-packages: com.example.listener");
    }

    @Test
    @DisplayName("Should contain eventFlowDisabledLogger bean when disabled")
    void shouldContainEventFlowDisabledLoggerBean() {
        // then
        assertThat(context.getBeanNamesForType(EventFlowDisabledAutoConfiguration.EventFlowDisabledLogger.class).length).isEqualTo(1);
        assertThat(context.getBean(EventFlowDisabledAutoConfiguration.EventFlowDisabledLogger.class)).isNotNull();
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestConfig {
    }
}
