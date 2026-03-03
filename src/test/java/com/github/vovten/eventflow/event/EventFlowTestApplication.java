package com.github.vovten.eventflow.event;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * Test application configuration for integration tests
 * Removed eventQueue bean to avoid conflict with EventDispatcherConfig
 */
@SpringBootApplication
@ComponentScan(
    basePackages = "com.github.vovten.eventflow.event",
    excludeFilters = @ComponentScan.Filter(
        type = FilterType.REGEX,
        pattern = ".*IntegrationTest.*"
    )
)
public class EventFlowTestApplication {
}
