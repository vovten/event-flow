package com.github.vovten.eventflow.autoconfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.support.TestPropertySourceUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link EventFlowDisabledAutoConfiguration}.
 */
class EventFlowDisabledAutoConfigurationTest {

    @Test
    @DisplayName("EventFlowDisabledAutoConfiguration should be created when event-flow.enabled is false")
    void eventFlowDisabledAutoConfigurationShouldBeCreatedWhenDisabled() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=false");

            context.register(EventFlowDisabledAutoConfiguration.class);
            context.refresh();

            // when
            boolean hasLogger = context.containsBean("eventFlowDisabledLogger");

            // then
            assertThat(hasLogger).isTrue();
        }
    }

    @Test
    @DisplayName("EventFlowDisabledAutoConfiguration should NOT be created when event-flow.enabled is true")
    void eventFlowDisabledAutoConfigurationShouldNotBeCreatedWhenEnabled() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");

            context.register(EventFlowDisabledAutoConfiguration.class);
            context.refresh();

            // when
            boolean hasLogger = context.containsBean("eventFlowDisabledLogger");

            // then
            assertThat(hasLogger).isFalse();
        }
    }

    @Test
    @DisplayName("EventFlowDisabledLogger should not throw exception on event")
    void eventFlowDisabledLoggerShouldNotThrowExceptionOnEvent() {
        // given
        EventFlowDisabledAutoConfiguration.EventFlowDisabledLogger logger =
                new EventFlowDisabledAutoConfiguration.EventFlowDisabledLogger();

        // when & then - should not throw
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() ->
                logger.onApplicationEvent(null)
        );
    }

    @Configuration
    static class TestConfig {
        @Bean
        public EventFlowDisabledAutoConfiguration.EventFlowDisabledLogger eventFlowDisabledLogger() {
            return new EventFlowDisabledAutoConfiguration.EventFlowDisabledLogger();
        }

        @Bean
        public EventFlowProperties eventFlowProperties() {
            return new EventFlowProperties();
        }
    }
}
