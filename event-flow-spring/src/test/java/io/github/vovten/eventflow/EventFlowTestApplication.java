package io.github.vovten.eventflow;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * Test application configuration for integration tests
 * Removed eventQueue bean to avoid conflict with EventDispatcherConfig
 * @since 1.0.0
 */
@SpringBootApplication
@ComponentScan(
        basePackages = "io.github.vovten.eventflow",
        excludeFilters = {
                @ComponentScan.Filter(
                        type = FilterType.REGEX,
                        pattern = ".*EventListenerRegistryIntegrationTest.*"
                ),
                @ComponentScan.Filter(
                        type = FilterType.REGEX,
                        pattern = ".*NoEventBeansConditionTest.*"
                )
        }
)
public class EventFlowTestApplication {
}
