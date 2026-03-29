package com.github.vovten.eventflow.autoconfig;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration that logs helpful hint when Event Flow is disabled.
 * <p>
 * This configuration is only created when {@code event-flow.enabled=false}
 * (which is the default). It provides a quick reference for enabling
 * Event Flow auto-configuration.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-29
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "false", matchIfMissing = true)
public class EventFlowDisabledAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public EventFlowDisabledLogger eventFlowDisabledLogger() {
        return new EventFlowDisabledLogger();
    }

    /**
     * Listener that logs configuration hint when application is ready.
     */
    public static class EventFlowDisabledLogger implements ApplicationListener<ApplicationReadyEvent> {

        private static final String MESSAGE = """
               ╔═══════════════════════════════════════════════════════════╗
               ║ Event Flow is disabled                                    ║
               ║ To enable Event Flow auto-configuration, add:             ║
               ╚═══════════════════════════════════════════════════════════╝
                event-flow:
                  enabled: true
                  scan-packages: com.example.listener
                  publisher:
                    enabled: true
                    channels:
                      - name: internal
                        transports:
                          - name: local-queue
                  dispatcher:
                    enabled: true
                    transports:
                      - name: local-queue
               ╔═══════════════════════════════════════════════════════════╗
               ║ This is a minimal configuration example. For detailed     ║
               ║ configuration options, see event-flow.yml and default     ║
               ║ values in EventFlowProperties class.                      ║
               ╚═══════════════════════════════════════════════════════════╝
                """;

        @Override
        public void onApplicationEvent(ApplicationReadyEvent event) {
            log.info(MESSAGE);
        }
    }
}
