package io.github.vovten.eventflow.autoconfig;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 *     lifecycle:
 *       enabled: false
 *       service-name: ""
 *       store:
 *         type: db
 *         table-name: event_store
 *         auto-init-schema: true
 *       retry:
 *         enabled: true
 *         max-retries: 3
 *         retry-interval: 30s
 *         min-age: 30s
 *     circuit-breaker:
 *       enabled: false
 *       failure-threshold: 10
 *       failure-rate-threshold: 0.8
 *       cooldown: 60s
 *       half-open-max-attempts: 3
 *       max-cache-size: 1000
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
        private CircuitBreakerConfig circuitBreaker = new CircuitBreakerConfig();
        private LifecyclePublisherConfig lifecycle = new LifecyclePublisherConfig();
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

        public CircuitBreakerConfig getCircuitBreaker() {
            return circuitBreaker;
        }

        public void setCircuitBreaker(CircuitBreakerConfig circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        public LifecyclePublisherConfig getLifecycle() {
            return lifecycle;
        }

        public void setLifecycle(LifecyclePublisherConfig lifecycle) {
            this.lifecycle = lifecycle;
        }

        public List<ChannelConfig> getChannels() {
            return channels;
        }

        public void setChannels(List<ChannelConfig> channels) {
            this.channels = channels;
        }
    }

    /**
     * Circuit breaker configuration for event publishing.
     * <p>
     * When enabled, the circuit breaker monitors publish failures per event type.
     * If the failure rate exceeds {@code failure-rate-threshold} within the
     * evaluation window ({@code failure-threshold} requests), the circuit opens
     * and subsequent publishes for that event type are rejected immediately.
     * After {@code cooldown}, limited attempts are allowed in half-open state
     * to probe recovery.
     * <p>
     * The circuit breaker wraps the transport publisher chain and sits between
     * the lifecycle publisher and the transport layer. Events are persisted
     * before the breaker check, so no events are lost when the circuit is open.
     * The scheduler can bypass the breaker via a dedicated mechanism.
     * <p>
     * Disabled by default — enable explicitly in production environments with
     * external service dependencies.
     */
    public static class CircuitBreakerConfig {
        /**
         * Enable circuit breaker protection for event publishing.
         */
        private boolean enabled = false;

        /**
         * Minimum number of requests before failure rate is evaluated.
         * The breaker waits for at least this many requests before checking
         * the failure rate against the threshold.
         */
        private int failureThreshold = 10;

        /**
         * Failure rate threshold (0.0–1.0) that triggers circuit opening.
         * E.g., {@code 0.8} means 80% of requests must fail to open.
         */
        private double failureRateThreshold = 0.8;

        /**
         * Duration to wait before transitioning from OPEN to HALF_OPEN state.
         * During this period, all requests are rejected immediately.
         */
        private Duration cooldown = Duration.ofSeconds(60);

        /**
         * Maximum number of failed attempts in HALF_OPEN state before
         * the circuit re-opens. A single success in HALF_OPEN closes the circuit.
         */
        private int halfOpenMaxAttempts = 3;

        /**
         * Maximum number of circuit breaker entries in the internal cache.
         * When the cache is full, the least recently used CLOSED breakers are
         * evicted automatically. OPEN and HALF_OPEN breakers remain active
         * because they are accessed on every publish attempt.
         */
        private int maxCacheSize = 1000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getFailureThreshold() {
            return failureThreshold;
        }

        public void setFailureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }

        public double getFailureRateThreshold() {
            return failureRateThreshold;
        }

        public void setFailureRateThreshold(double failureRateThreshold) {
            this.failureRateThreshold = failureRateThreshold;
        }

        public Duration getCooldown() {
            return cooldown;
        }

        public void setCooldown(Duration cooldown) {
            this.cooldown = cooldown;
        }

        public int getHalfOpenMaxAttempts() {
            return halfOpenMaxAttempts;
        }

        public void setHalfOpenMaxAttempts(int halfOpenMaxAttempts) {
            this.halfOpenMaxAttempts = halfOpenMaxAttempts;
        }

        public int getMaxCacheSize() {
            return maxCacheSize;
        }

        public void setMaxCacheSize(int maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
        }

        @Override
        public String toString() {
            return "CircuitBreakerConfig{" +
                    "enabled=" + enabled +
                    ", failureThreshold=" + failureThreshold +
                    ", failureRateThreshold=" + failureRateThreshold +
                    ", cooldown=" + cooldown +
                    ", halfOpenMaxAttempts=" + halfOpenMaxAttempts +
                    ", maxCacheSize=" + maxCacheSize +
                    '}';
        }
    }

    /**
     * Lifecycle-aware publisher configuration for event lifecycle tracking.
     * <p>
     * When enabled, events are persisted to an {@code EventStore} according to their
     * {@code EventLifecycle} level:
     * <ul>
     *   <li>{@code PERSISTED} — saved with {@code UNDEFINED} status, no further tracking</li>
     *   <li>{@code MANAGED} — saved with {@code NEW} status, full lifecycle tracking
     *       via acknowledgment events</li>
     * </ul>
     * <p>
     * Requires a {@code DataSource} bean to be present for the {@code "db"} store type.
     */
    public static class LifecyclePublisherConfig {
        private boolean enabled = false;
        private String serviceName = "";
        private StoreConfig store = new StoreConfig();
        private RetryTrackingConfig retry = new RetryTrackingConfig();
        private CleanupConfig cleanup = new CleanupConfig();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public StoreConfig getStore() {
            return store;
        }

        public void setStore(StoreConfig store) {
            this.store = store;
        }

        public RetryTrackingConfig getRetry() {
            return retry;
        }

        public void setRetry(RetryTrackingConfig retry) {
            this.retry = retry;
        }

        public CleanupConfig getCleanup() {
            return cleanup;
        }

        public void setCleanup(CleanupConfig cleanup) {
            this.cleanup = cleanup;
        }

        @Override
        public String toString() {
            return "LifecyclePublisherConfig{" +
                    "enabled=" + enabled +
                    ", serviceName='" + serviceName + '\'' +
                    ", store=" + store +
                    ", retry=" + retry +
                    ", cleanup=" + cleanup +
                    '}';
        }

        /**
         * Event store configuration.
         */
        public static class StoreConfig {
            /**
             * Store type identifier. Built-in values: {@code "db"}, {@code "in-memory"}.
             * Custom types are supported by defining a custom {@code @Bean EventStore}
             * — the framework auto-discovers it and returns it when the type matches.
             */
            private String type = "db";

            /**
             * Name of the database table for event storage (only for {@code type: "db"}).
             */
            private String tableName = "event_store";

            /**
             * Automatically create the table on startup (only for {@code type: "db"}).
             * Set to false in production and manage schema via Flyway/Liquibase.
             */
            private boolean autoInitSchema = false;

            public String getType() {
                return type;
            }

            public void setType(String type) {
                this.type = type;
            }

            public String getTableName() {
                return tableName;
            }

            public void setTableName(String tableName) {
                this.tableName = tableName;
            }

            public boolean isAutoInitSchema() {
                return autoInitSchema;
            }

            public void setAutoInitSchema(boolean autoInitSchema) {
                this.autoInitSchema = autoInitSchema;
            }

            @Override
            public String toString() {
                return "StoreConfig{" +
                        "type='" + type + '\'' +
                        ", tableName='" + tableName + '\'' +
                        ", autoInitSchema=" + autoInitSchema +
                        '}';
            }
        }

        /**
         * Retry configuration for lifecycle event tracking.
         * <p>
         * Controls automatic retry of failed and stuck (PUBLISHED) events.
         */
        public static class RetryTrackingConfig {
            private boolean enabled = true;
            private int maxRetries = 3;
            private Duration retryInterval = Duration.ofSeconds(30);
            private Duration minAge = Duration.ofSeconds(30);
            private int batchSize = 1000;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public int getMaxRetries() {
                return maxRetries;
            }

            public void setMaxRetries(int maxRetries) {
                this.maxRetries = maxRetries;
            }

            public Duration getRetryInterval() {
                return retryInterval;
            }

            public void setRetryInterval(Duration retryInterval) {
                this.retryInterval = retryInterval;
            }

            public Duration getMinAge() {
                return minAge;
            }

            public void setMinAge(Duration minAge) {
                this.minAge = minAge;
            }

            public int getBatchSize() {
                return batchSize;
            }

            public void setBatchSize(int batchSize) {
                this.batchSize = batchSize;
            }

            @Override
            public String toString() {
                return "RetryTrackingConfig{" +
                        "enabled=" + enabled +
                        ", maxRetries=" + maxRetries +
                        ", retryInterval=" + retryInterval +
                        ", minAge=" + minAge +
                        ", batchSize=" + batchSize +
                        '}';
            }
        }

        /**
         * Cleanup configuration for lifecycle event tracking.
         * <p>
         * Controls automatic deletion of old terminal events (HANDLED, UNDEFINED)
         * from the event store. Events older than {@code maxAge} are deleted
         * in batches of {@code batchSize} with a pause between batches to
         * reduce database load.
         * <p>
         * Only terminal events that are no longer needed for retry or tracking
         * are cleaned up. {@code FAILED} events are preserved for manual inspection.
         */
        public static class CleanupConfig {
            /**
             * Enable the periodic cleanup scheduler.
             */
            private boolean enabled = false;

            /**
             * How often the cleanup cycle runs.
             */
            private Duration interval = Duration.ofMinutes(60);

            /**
             * Events older than this duration are eligible for deletion.
             */
            private Duration maxAge = Duration.ofDays(7);

            /**
             * Maximum number of events to delete in a single batch.
             */
            private int batchSize = 1000;

            /**
             * Pause between consecutive batches to spread database load.
             */
            private Duration pauseBetweenBatches = Duration.ofMillis(100);

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public Duration getInterval() {
                return interval;
            }

            public void setInterval(Duration interval) {
                this.interval = interval;
            }

            public Duration getMaxAge() {
                return maxAge;
            }

            public void setMaxAge(Duration maxAge) {
                this.maxAge = maxAge;
            }

            public int getBatchSize() {
                return batchSize;
            }

            public void setBatchSize(int batchSize) {
                this.batchSize = batchSize;
            }

            public Duration getPauseBetweenBatches() {
                return pauseBetweenBatches;
            }

            public void setPauseBetweenBatches(Duration pauseBetweenBatches) {
                this.pauseBetweenBatches = pauseBetweenBatches;
            }

            @Override
            public String toString() {
                return "CleanupConfig{" +
                        "enabled=" + enabled +
                        ", interval=" + interval +
                        ", maxAge=" + maxAge +
                        ", batchSize=" + batchSize +
                        ", pauseBetweenBatches=" + pauseBetweenBatches +
                        '}';
            }
        }
    }

    /**
     * Logging configuration for event publishing.
     */
    public static class LoggingConfig {
        private boolean enabled = false;
        private int maxPayloadLength = 500;
        /**
         * List of event simple class names to exclude from logging.
         * Default: SuccessAck, FailureAck (lifecycle acknowledgment events).
         */
        private List<String> excludedEvents = new ArrayList<>(List.of("SuccessAck", "FailureAck"));
        /**
         * Per-event log level overrides.
         * Key: event simple class name (e.g., "HeartbeatEvent").
         * Value: log level name (DEBUG, INFO, WARN, ERROR).
         * When set, overrides the default status-based log level for matching events.
         */
        private Map<String, String> logLevels = new HashMap<>();

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

        public List<String> getExcludedEvents() {
            return excludedEvents;
        }

        public void setExcludedEvents(List<String> excludedEvents) {
            this.excludedEvents = excludedEvents;
        }

        public Map<String, String> getLogLevels() {
            return logLevels;
        }

        public void setLogLevels(Map<String, String> logLevels) {
            this.logLevels = logLevels;
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
        private LifecycleConfig lifecycle = new LifecycleConfig();

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

        public LifecycleConfig getLifecycle() {
            return lifecycle;
        }

        public void setLifecycle(LifecycleConfig lifecycle) {
            this.lifecycle = lifecycle;
        }
    }

    /**
     * Lifecycle configuration for the dispatcher.
     * <p>
     * When enabled, the dispatcher publishes {@code SuccessAck} or
     * {@code FailureAck} acknowledgment events back to the source channels
     * after handler execution, enabling the publisher to track event lifecycle.
     */
    public static class LifecycleConfig {
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        @Override
        public String toString() {
            return "LifecycleConfig{enabled=" + enabled + '}';
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
