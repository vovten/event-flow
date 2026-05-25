package io.github.vovten.eventflow.autoconfig;

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
 *   dispatcher.listener-packages: com.example.listener
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
 *           - name: local-queue
 *             capacity: 1000
 *       - name: external
 *         transports:
 *           - name: kafka
 *             topics: events-topic
 *             servers: localhost:9092
 *   dispatcher:
 *     enabled: true
 *     idempotent:
 *       enabled: true
 *       ttl: 10m
 *       max-size: 10000
 *       warn-on-duplicate: true
 *     thread-pool:
 *       core-size: 4
 *       max-size: 16
 *       queue-capacity: 100
 *       keep-alive-seconds: 60
 *       concurrency-limit: 50
 *     transports:
 *       - name: local-queue
 *         capacity: 1000
 *       - name: kafka
 *         topic: events-topic
 *         servers: localhost:9092
 *         consumerGroup: event-flow-group
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "event-flow")
public class EventFlowProperties {

    /**
     * Enable or disable Event Flow auto-configuration completely.
     */
    private boolean enabled = false;

    /**
     * Publisher configuration.
     */
    private PublisherConfig publisher = new PublisherConfig();

    /**
     * Dispatcher configuration.
     */
    private DispatcherConfig dispatcher = new DispatcherConfig();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public PublisherConfig getPublisher() {
        return publisher;
    }

    public void setPublisher(PublisherConfig publisher) {
        this.publisher = publisher;
    }

    public DispatcherConfig getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(DispatcherConfig dispatcher) {
        this.dispatcher = dispatcher;
    }

    /**
     * Publisher configuration settings.
     */
    public static class PublisherConfig {
        private boolean enabled = false;
        private boolean transactional = true;
        private LoggingConfig logging = new LoggingConfig();
        private RetryConfig retry = new RetryConfig();
        private List<ChannelConfig> channels = new ArrayList<>();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isTransactional() {
            return transactional;
        }

        public void setTransactional(boolean transactional) {
            this.transactional = transactional;
        }

        public LoggingConfig getLogging() {
            return logging;
        }

        public void setLogging(LoggingConfig logging) {
            this.logging = logging;
        }

        public RetryConfig getRetry() {
            return retry;
        }

        public void setRetry(RetryConfig retry) {
            this.retry = retry;
        }

        public List<ChannelConfig> getChannels() {
            return channels;
        }

        public void setChannels(List<ChannelConfig> channels) {
            this.channels = channels;
        }
    }

    /**
     * Logging configuration for event publishing.
     */
    public static class LoggingConfig {
        private boolean enabled = false;
        private int maxPayloadLength = 500;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxPayloadLength() {
            return maxPayloadLength;
        }

        public void setMaxPayloadLength(int maxPayloadLength) {
            this.maxPayloadLength = maxPayloadLength;
        }
    }

    /**
     * Retry configuration for event publishing.
     */
    public static class RetryConfig {
        private boolean enabled = false;
        private int maxAttempts = 3;
        private Duration initialDelay = Duration.ofMillis(100);
        private double multiplier = 2.0;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public void setInitialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }
    }

    /**
     * Channel configuration for event publishing.
     */
    public static class ChannelConfig {
        private String name = "internal";
        /**
         * List of transport configurations.
         * Transport name identifies the type (e.g., "local-queue", "kafka").
         */
        private List<TransportConfig> transports = new ArrayList<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<TransportConfig> getTransports() {
            return transports;
        }

        public void setTransports(List<TransportConfig> transports) {
            this.transports = transports;
        }
    }

    /**
     * Dispatcher configuration settings.
     */
    public static class DispatcherConfig {
        private boolean enabled = false;
        /**
         * Package(s) to scan for @EventListener annotated beans.
         */
        private String listenerPackages = "";
        private ThreadPoolConfig threadPool = new ThreadPoolConfig();
        private List<TransportConfig> transports = new ArrayList<>();
        private IdempotentConfig idempotent = new IdempotentConfig();
        private LoggingConfig logging = new LoggingConfig();
        private DeserializationConfig deserialization = new DeserializationConfig();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getListenerPackages() {
            return listenerPackages;
        }

        public void setListenerPackages(String listenerPackages) {
            this.listenerPackages = listenerPackages;
        }

        public ThreadPoolConfig getThreadPool() {
            return threadPool;
        }

        public void setThreadPool(ThreadPoolConfig threadPool) {
            this.threadPool = threadPool;
        }

        public List<TransportConfig> getTransports() {
            return transports;
        }

        public void setTransports(List<TransportConfig> transports) {
            this.transports = transports;
        }

        public IdempotentConfig getIdempotent() {
            return idempotent;
        }

        public void setIdempotent(IdempotentConfig idempotent) {
            this.idempotent = idempotent;
        }

        public LoggingConfig getLogging() {
            return logging;
        }

        public void setLogging(LoggingConfig logging) {
            this.logging = logging;
        }

        public DeserializationConfig getDeserialization() {
            return deserialization;
        }

        public void setDeserialization(DeserializationConfig deserialization) {
            this.deserialization = deserialization;
        }
    }

    /**
     * Idempotent configuration for dispatcher.
     */
    public static class IdempotentConfig {
        private boolean enabled = false;
        private Duration ttl = Duration.ofMinutes(10);
        private long maxSize = 10_000;
        private boolean warnOnDuplicate = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }

        public long getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(long maxSize) {
            this.maxSize = maxSize;
        }

        public boolean isWarnOnDuplicate() {
            return warnOnDuplicate;
        }

        public void setWarnOnDuplicate(boolean warnOnDuplicate) {
            this.warnOnDuplicate = warnOnDuplicate;
        }
    }

    /**
     * Thread pool configuration for dispatcher.
     */
    public static class ThreadPoolConfig {
        private int coreSize = 4;
        private int maxSize = 16;
        private int queueCapacity = 100;
        private int keepAliveSeconds = 60;
        /**
         * Maximum number of concurrent handler executions.
         * Provides backpressure when using virtual thread executors.
         * When 0 (default), no limit is applied.
         * <p>
         * <b>Recommended values:</b>
         * <ul>
         *   <li>I/O-bound handlers (DB, HTTP): 20-100</li>
         *   <li>CPU-bound handlers: number of CPU cores</li>
         *   <li>Mixed workload: 50-200</li>
         * </ul>
         */
        private int concurrencyLimit = 0;

        public int getCoreSize() {
            return coreSize;
        }

        public void setCoreSize(int coreSize) {
            this.coreSize = coreSize;
        }

        public int getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(int maxSize) {
            this.maxSize = maxSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public int getKeepAliveSeconds() {
            return keepAliveSeconds;
        }

        public void setKeepAliveSeconds(int keepAliveSeconds) {
            this.keepAliveSeconds = keepAliveSeconds;
        }

        public int getConcurrencyLimit() {
            return concurrencyLimit;
        }

        public void setConcurrencyLimit(int concurrencyLimit) {
            this.concurrencyLimit = concurrencyLimit;
        }
    }

    /**
     * Transport configuration.
     * Name identifies the transport type (e.g., "local-queue", "kafka").
     */
    public static class TransportConfig {
        private String name = "local-queue";
        private int capacity = 1000;
        private String topics;
        private String servers;
        private String consumerGroup = "event-flow-group";
        /**
         * Serialization format for event transport.
         * Supported values: "json" (default), "msgpack"
         */
        private String serialization = "json";

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getCapacity() {
            return capacity;
        }

        public void setCapacity(int capacity) {
            this.capacity = capacity;
        }

        public String getTopics() {
            return topics;
        }

        public void setTopics(String topics) {
            this.topics = topics;
        }

        public String getServers() {
            return servers;
        }

        public void setServers(String servers) {
            this.servers = servers;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        public String getSerialization() {
            return serialization;
        }

        public void setSerialization(String serialization) {
            this.serialization = serialization;
        }
    }

    /**
     * Deserialization configuration for event security.
     */
    public static class DeserializationConfig {
        /**
         * List of allowed event packages for deserialization.
         * Events from these packages can be safely deserialized.
         * Default: io.github.vovten.eventflow
         */
        private List<String> allowedEventPackages = new ArrayList<>(List.of("io.github.vovten.eventflow"));

        public List<String> getAllowedEventPackages() {
            return allowedEventPackages;
        }

        public void setAllowedEventPackages(List<String> allowedEventPackages) {
            this.allowedEventPackages = allowedEventPackages;
        }
    }
}
