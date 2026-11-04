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

    /**
     * Processing-retry configuration.
     */
    private EventRetryConfig retry = new EventRetryConfig();

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

    public EventRetryConfig getRetry() {
        return retry;
    }

    public void setRetry(EventRetryConfig retry) {
        this.retry = retry;
    }

    /**
     * Publisher configuration settings.
     */
    public static class PublisherConfig {
        private boolean enabled = false;
        private boolean transactional = true;
        private LoggingConfig logging = new LoggingConfig();
        private RetryConfig retry = new RetryConfig();
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

            /**
             * Retention for dead-lettered events. Must be longer than {@code maxAge}.
             */
            private Duration deadLetterMaxAge = Duration.ofDays(30);

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

            public Duration getDeadLetterMaxAge() {
                return deadLetterMaxAge;
            }

            public void setDeadLetterMaxAge(Duration deadLetterMaxAge) {
                this.deadLetterMaxAge = deadLetterMaxAge;
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
         * Default: AcceptedAck, NoHandlerAck, SuccessAck, FailureAck
         * (lifecycle acknowledgment events).
         */
        private List<String> excludedEvents = new ArrayList<>(
                List.of("AcceptedAck", "NoHandlerAck", "SuccessAck", "FailureAck"));
        /**
         * Per-event minimum log level for suppressing low-severity entries.
         * Key: payload simple class name or fully-qualified class name (e.g., "HeartbeatEvent"
         * or "io.example.HeartbeatEvent"). Fully-qualified keys are validated at startup;
         * invalid entries fail fast with an IllegalArgumentException.
         * Value: minimum level threshold; outcomes below it are skipped.
         *   - ERROR: log only failures
         *   - WARN:  log failures and partial success
         *   - INFO/DEBUG/TRACE: no suppression (default)
         * The entry is still emitted at its natural status-based level
         * (INFO=success, WARN=partial, ERROR=failure); the value only filters it out.
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

        /**
         * Kafka record key strategy for outgoing Kafka transports.
         * Determines which Kafka partition receives the event.
         * Supported values:
         * - "none" (default): no key, Kafka round-robin
         * - "process-id": key is the event's processId (all events of a process → one partition)
         * - "event-type": key is the event type class name (all events of a type → one partition)
         * - "metadata:&lt;name&gt;": key is the named value from the Envelope metadata
         * - "bean:&lt;beanName&gt;": key resolved by a user-defined {@code EventKeyResolver} Spring bean
         */
        private String key = "none";

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

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
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

    /**
     * Two-domain retry configuration.
     */
    public static class EventRetryConfig {

        private DeliveryRetryConfig delivery = new DeliveryRetryConfig();
        private ProcessingRetryConfig processing = new ProcessingRetryConfig();

        public DeliveryRetryConfig getDelivery() {
            return delivery;
        }

        public void setDelivery(DeliveryRetryConfig delivery) {
            this.delivery = delivery;
        }

        public ProcessingRetryConfig getProcessing() {
            return processing;
        }

        public void setProcessing(ProcessingRetryConfig processing) {
            this.processing = processing;
        }
    }

    /**
     * Durable delivery-retry configuration.
     * <p>
     * Delivery retries are publisher-side: they recover events that were not
     * accepted by the consumer. The new durable mechanism is opt-in.
     */
    public static class DeliveryRetryConfig {

        /**
         * Enable durable delivery retries.
         */
        private boolean enabled = false;

        /**
         * Delay between delivery retry cycles.
         */
        private Duration interval = Duration.ofSeconds(30);

        /**
         * Maximum time to wait for an acceptance ack.
         */
        private Duration ackTimeout = Duration.ofSeconds(60);

        /**
         * Maximum number of retries after the first publish.
         */
        private int maxRetries = 3;

        /**
         * Maximum number of retry rows handled per cycle.
         */
        private int batchSize = 100;

        /**
         * Base delay before the first durable delivery retry.
         */
        private Duration retryDelay = Duration.ofSeconds(30);

        /**
         * Whether publish success requires an explicit {@code AcceptedAck}.
         * Set to {@code false} during a mixed-fleet migration.
         */
        private boolean requireAcceptedAck = true;

        /**
         * Retry table name.
         */
        private String tableName = "event_retry";

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

        public Duration getAckTimeout() {
            return ackTimeout;
        }

        public void setAckTimeout(Duration ackTimeout) {
            this.ackTimeout = ackTimeout;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getRetryDelay() {
            return retryDelay;
        }

        public void setRetryDelay(Duration retryDelay) {
            this.retryDelay = retryDelay;
        }

        public boolean isRequireAcceptedAck() {
            return requireAcceptedAck;
        }

        public void setRequireAcceptedAck(boolean requireAcceptedAck) {
            this.requireAcceptedAck = requireAcceptedAck;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }
    }

    /**
     * Processing-retry settings, including the default policy and named overrides.
     */
    public static class ProcessingRetryConfig {

        /**
         * Enable local processing retries.
         */
        private boolean enabled = true;

        /**
         * Absolute cap for local retry attempts, independent of listener policies.
         */
        private int localRetryCap = 10;

        /**
         * Enable durable processing retries. Requires delivery retries.
         */
        private boolean durableEnabled = false;

        /**
         * Delay between durable processing retry cycles.
         */
        private Duration durableInterval = Duration.ofSeconds(30);

        /**
         * Maximum time to wait for a final ack of a durable processing retry.
         */
        private Duration durableAckTimeout = Duration.ofMinutes(5);

        /**
         * Global hard cap for durable processing retries after the first attempt.
         */
        private int durableMaxRetries = 3;

        /**
         * Maximum number of durable processing retry rows handled per cycle.
         */
        private int durableBatchSize = 100;

        private DefaultRetryPolicyConfig defaultPolicy = new DefaultRetryPolicyConfig();

        /**
         * Named policy overrides by policy name.
         */
        private Map<String, RetryPolicyOverrideConfig> policies = new HashMap<>();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getLocalRetryCap() {
            return localRetryCap;
        }

        public void setLocalRetryCap(int localRetryCap) {
            this.localRetryCap = localRetryCap;
        }

        public boolean isDurableEnabled() {
            return durableEnabled;
        }

        public void setDurableEnabled(boolean durableEnabled) {
            this.durableEnabled = durableEnabled;
        }

        public Duration getDurableInterval() {
            return durableInterval;
        }

        public void setDurableInterval(Duration durableInterval) {
            this.durableInterval = durableInterval;
        }

        public Duration getDurableAckTimeout() {
            return durableAckTimeout;
        }

        public void setDurableAckTimeout(Duration durableAckTimeout) {
            this.durableAckTimeout = durableAckTimeout;
        }

        public int getDurableMaxRetries() {
            return durableMaxRetries;
        }

        public void setDurableMaxRetries(int durableMaxRetries) {
            this.durableMaxRetries = durableMaxRetries;
        }

        public int getDurableBatchSize() {
            return durableBatchSize;
        }

        public void setDurableBatchSize(int durableBatchSize) {
            this.durableBatchSize = durableBatchSize;
        }

        public DefaultRetryPolicyConfig getDefaultPolicy() {
            return defaultPolicy;
        }

        public void setDefaultPolicy(DefaultRetryPolicyConfig defaultPolicy) {
            this.defaultPolicy = defaultPolicy;
        }

        public Map<String, RetryPolicyOverrideConfig> getPolicies() {
            return policies;
        }

        public void setPolicies(Map<String, RetryPolicyOverrideConfig> policies) {
            this.policies = policies;
        }
    }

    /**
     * Default processing-retry policy.
     */
    public static class DefaultRetryPolicyConfig {

        private int maxRetries = 3;
        private int localMaxRetries = 0;
        private Duration initialDelay = Duration.ofSeconds(30);
        private double multiplier = 2.0;
        private Duration maxDelay = Duration.ofMinutes(10);
        private double jitter = 0.5;
        private List<String> retryOn = new ArrayList<>();
        private List<String> noRetryOn = new ArrayList<>();

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public int getLocalMaxRetries() {
            return localMaxRetries;
        }

        public void setLocalMaxRetries(int localMaxRetries) {
            this.localMaxRetries = localMaxRetries;
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

        public Duration getMaxDelay() {
            return maxDelay;
        }

        public void setMaxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
        }

        public double getJitter() {
            return jitter;
        }

        public void setJitter(double jitter) {
            this.jitter = jitter;
        }

        public List<String> getRetryOn() {
            return retryOn;
        }

        public void setRetryOn(List<String> retryOn) {
            this.retryOn = retryOn;
        }

        public List<String> getNoRetryOn() {
            return noRetryOn;
        }

        public void setNoRetryOn(List<String> noRetryOn) {
            this.noRetryOn = noRetryOn;
        }
    }

    /**
     * Partial override of a named processing-retry policy.
     */
    public static class RetryPolicyOverrideConfig {

        private Integer maxRetries;
        private Integer localMaxRetries;
        private Duration initialDelay;
        private Double multiplier;
        private Duration maxDelay;
        private Double jitter;
        private List<String> retryOn;
        private List<String> noRetryOn;

        public Integer getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(Integer maxRetries) {
            this.maxRetries = maxRetries;
        }

        public Integer getLocalMaxRetries() {
            return localMaxRetries;
        }

        public void setLocalMaxRetries(Integer localMaxRetries) {
            this.localMaxRetries = localMaxRetries;
        }

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public void setInitialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
        }

        public Double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(Double multiplier) {
            this.multiplier = multiplier;
        }

        public Duration getMaxDelay() {
            return maxDelay;
        }

        public void setMaxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
        }

        public Double getJitter() {
            return jitter;
        }

        public void setJitter(Double jitter) {
            this.jitter = jitter;
        }

        public List<String> getRetryOn() {
            return retryOn;
        }

        public void setRetryOn(List<String> retryOn) {
            this.retryOn = retryOn;
        }

        public List<String> getNoRetryOn() {
            return noRetryOn;
        }

        public void setNoRetryOn(List<String> noRetryOn) {
            this.noRetryOn = noRetryOn;
        }
    }
}
