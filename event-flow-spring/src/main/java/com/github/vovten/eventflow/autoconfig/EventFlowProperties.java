package com.github.vovten.eventflow.autoconfig;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for Event Flow auto-configuration.
 * <p>
 * Provides flexible configuration for publisher and dispatcher components
 * with the ability to completely disable auto-configuration.
 * <p>
 * <b>Usage example (application.yml):</b>
 * <pre>{@code
 * event-flow:
 *   enabled: true
 *   scan-packages: com.example.listener
 *   publisher:
 *     enabled: true
 *     transactional: true
 *     retry:
 *       enabled: true
 *       max-attempts: 3
 *       initial-delay: 100ms
 *       multiplier: 2.0
 *     channels:
 *       - name: internal
 *         transports:
 *           - name: in-memory
 *             capacity: 1000
 *       - name: external
 *         transports:
 *           - name: kafka
 *             topic: events-topic
 *             bootstrapServers: localhost:9092
 *   dispatcher:
 *     enabled: true
 *     thread-pool:
 *       core-size: 4
 *       max-size: 16
 *       queue-capacity: 100
 *       keep-alive-seconds: 60
 *     transports:
 *       - name: in-memory
 *         capacity: 1000
 *       - name: kafka
 *         topic: events-topic
 *         bootstrapServers: localhost:9092
 *         consumerGroup: event-flow-group
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@Data
@ConfigurationProperties(prefix = "event-flow")
public class EventFlowProperties {

    /**
     * Enable or disable Event Flow auto-configuration completely.
     */
    private boolean enabled = true;

    /**
     * Package(s) to scan for @EventListener annotated beans.
     */
    private String scanPackages = "";

    /**
     * Publisher configuration.
     */
    private PublisherConfig publisher = new PublisherConfig();

    /**
     * Dispatcher configuration.
     */
    private DispatcherConfig dispatcher = new DispatcherConfig();

    /**
     * Publisher configuration settings.
     */
    @Data
    public static class PublisherConfig {
        private boolean enabled = true;
        private boolean transactional = true;
        private boolean silent = false;
        private RetryConfig retry = new RetryConfig();
        private List<ChannelConfig> channels = new ArrayList<>();
    }

    /**
     * Retry configuration for event publishing.
     */
    @Data
    public static class RetryConfig {
        private boolean enabled = false;
        private int maxAttempts = 3;
        private Duration initialDelay = Duration.ofMillis(100);
        private double multiplier = 2.0;
    }

    /**
     * Channel configuration for event publishing.
     */
    @Data
    public static class ChannelConfig {
        private String name = "internal";
        /**
         * List of transport configurations.
         * Transport name identifies the type (e.g., "in-memory", "kafka").
         */
        private List<TransportConfig> transports = new ArrayList<>();
    }

    /**
     * Dispatcher configuration settings.
     */
    @Data
    public static class DispatcherConfig {
        private boolean enabled = true;
        private ThreadPoolConfig threadPool = new ThreadPoolConfig();
        private List<TransportConfig> transports = new ArrayList<>();
    }

    /**
     * Thread pool configuration for dispatcher.
     */
    @Data
    public static class ThreadPoolConfig {
        private int coreSize = 4;
        private int maxSize = 16;
        private int queueCapacity = 100;
        private int keepAliveSeconds = 60;
    }

    /**
     * Transport configuration.
     * Name identifies the transport type (e.g., "in-memory", "kafka").
     */
    @Data
    public static class TransportConfig {
        private String name = "in-memory";
        private int capacity = 1000;
        private String topic;
        private String bootstrapServers;
        private String consumerGroup = "event-flow-group";
    }
}
