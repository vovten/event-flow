package com.github.vovten.eventflow.event;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Test application configuration for integration tests
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

    @Bean
    public BlockingDeque<Event> eventQueue() {
        return new LinkedBlockingDeque<>();
    }
}
