package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties.LifecyclePublisherConfig;
import io.github.vovten.eventflow.lifecycle.AckHandler;
import io.github.vovten.eventflow.lifecycle.AckHandlerOptions;
import io.github.vovten.eventflow.lifecycle.DeliveryRetryOptions;
import io.github.vovten.eventflow.lifecycle.DeliveryRetryScheduler;
import io.github.vovten.eventflow.lifecycle.LifecycleRepository;
import io.github.vovten.eventflow.lifecycle.ProcessingRetryOptions;
import io.github.vovten.eventflow.lifecycle.ProcessingRetryScheduler;
import io.github.vovten.eventflow.lifecycle.RetrySchedulerSupport;
import io.github.vovten.eventflow.lifecycle.RetryWatchdog;
import io.github.vovten.eventflow.lifecycle.RetryWatchdogOptions;
import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.InMemoryEventRetryStore;
import io.github.vovten.eventflow.lifecycle.store.InMemoryLifecycleRepository;
import io.github.vovten.eventflow.lifecycle.store.db.JdbcEventRetryStore;
import io.github.vovten.eventflow.lifecycle.store.db.JdbcLifecycleRepository;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.lifecycle.EventCleanupScheduler;
import io.github.vovten.eventflow.lifecycle.EventRetryScheduler;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.db.JdbcEventStore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.List;
import java.util.UUID;

/**
 * Auto-configuration for lifecycle-aware event publishing and lifecycle tracking.
 * <p>
 * Provides the following beans when {@code event-flow.publisher.lifecycle.enabled=true}:
 * <ul>
 *   <li>{@link EventStore} — resolves user-defined custom {@link EventStore} beans
 *       by the configured {@code store.type}, or creates a built-in implementation</li>
 *   <li>{@link AckHandler} — processes lifecycle ack events (SuccessAck/FailureAck)</li>
 *   <li>{@link EventRetryScheduler} — periodically retries failed events</li>
 * </ul>
 * <p>
 * Built-in store types:
 * <ul>
 *   <li>{@code "db"} — {@link io.github.vovten.eventflow.lifecycle.store.db.JdbcEventStore} (requires DataSource, schema auto-init)</li>
 *   <li>{@code "in-memory"} — {@link InMemoryEventStore} (non-persistent, for testing)</li>
 * </ul>
 * <p>
 * Custom stores: define a {@code @Bean EventStore} and set {@code store.type}
 * to match its {@link EventStore#getType()}. The bean will be auto-discovered
 * and returned by {@link #eventStore(ObjectProvider, ObjectProvider)}.
 * <p>
 * The {@link EventPublisher} from {@link PublisherConfiguration} is automatically
 * wrapped in an {@link io.github.vovten.eventflow.lifecycle.EventLifecyclePublisher}
 * when lifecycle-aware publishing is enabled.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle", name = "enabled", havingValue = "true")
public class LifecycleConfiguration {

    private static final Logger log = LoggerFactory.getLogger(LifecycleConfiguration.class);

    private final EventFlowProperties properties;

    public LifecycleConfiguration(EventFlowProperties properties) {
        this.properties = properties;
        new RetryConfigurationValidator().validate(properties);
    }

    /**
     * Resolves the active {@link EventStore} for the configured {@code store.type}.
     * <p>
     * Resolution order:
     * <ol>
     *   <li>If a user-defined {@code @Bean EventStore} matches {@code store.type},
     *       that bean is returned directly</li>
     *   <li>If {@code store.type} is {@code "in-memory"}, creates {@link InMemoryEventStore}</li>
     *   <li>Otherwise (default {@code "db"}), creates {@link JdbcEventStore} if a
     *       {@link DataSource} is available</li>
     * </ol>
     * <p>
     * Marked as {@link Primary @Primary} so this bean is used for injection into
     * {@link AckHandler}, {@link EventRetryScheduler}, and other consumers.
     *
     * @param customStores       provider for user-defined EventStore beans
     * @param dataSourceProvider provider for optional DataSource
     * @return the event store for the configured type
     * @throws IllegalStateException if no matching custom store is found and the
     *                               built-in type ("db") has no DataSource available
     */
    @Bean
    @Primary
    public EventStore eventStore(
            ObjectProvider<List<EventStore>> customStores,
            ObjectProvider<DataSource> dataSourceProvider) {
        String type = properties.getPublisher().getLifecycle().getStore().getType();
        List<EventStore> stores = customStores.getIfAvailable();
        if (stores != null) {
            for (EventStore store : stores) {
                if (type.equals(store.getType())) {
                    log.info("Matched custom EventStore: type='{}'", type);
                    return store;
                }
            }
        }
        if ("in-memory".equals(type)) {
            log.warn("Using InMemoryEventStore — event data is NOT persisted across restarts");
            return new InMemoryEventStore();
        }
        DataSource dataSource = dataSourceProvider.getIfAvailable();
        if (dataSource != null) {
            LifecyclePublisherConfig.StoreConfig cfg = properties.getPublisher().getLifecycle().getStore();
            log.info("Creating JdbcEventStore: table={}, autoInit={}", cfg.getTableName(), cfg.isAutoInitSchema());
            return new JdbcEventStore(dataSource, cfg.getTableName(), cfg.isAutoInitSchema());
        }
        throw new IllegalStateException(
                "store.type is '" + type + "' but no DataSource available. " +
                "Either configure a DataSource, set store.type to 'in-memory', " +
                "or define a custom @Bean EventStore with type '" + type + "'.");
    }

    /**
     * Creates the durable retry store.
     *
     * @param eventStore          the active event store
     * @param dataSourceProvider  optional DataSource
     * @return the retry store
     * @throws IllegalStateException if the event store is JDBC without a DataSource or is custom
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnExpression("${event-flow.retry.delivery.enabled:false} "
            + "or ${event-flow.retry.processing.durable-enabled:false}")
    public EventRetryStore eventRetryStore(EventStore eventStore,
                                           ObjectProvider<DataSource> dataSourceProvider) {
        if (eventStore instanceof JdbcEventStore) {
            DataSource dataSource = dataSourceProvider.getIfAvailable();
            if (dataSource == null) {
                throw new IllegalStateException(
                        "Durable retry requires a DataSource when the event store is JDBC");
            }
            String tableName = properties.getRetry().getDelivery().getTableName();
            boolean autoInit = properties.getPublisher().getLifecycle().getStore().isAutoInitSchema();
            log.info("Creating JdbcEventRetryStore: table={}, autoInit={}", tableName, autoInit);
            return new JdbcEventRetryStore(dataSource, tableName, autoInit);
        }
        if (eventStore instanceof InMemoryEventStore) {
            log.warn("Using InMemoryEventRetryStore — retry rows are NOT persisted");
            return new InMemoryEventRetryStore();
        }
        throw new IllegalStateException(
                "Durable retry requires a built-in EventStore; custom stores must provide "
                        + "their own LifecycleRepository");
    }

    /**
     * Creates the transactional lifecycle repository.
     *
     * @param eventStore         the active event store
     * @param retryStore         the durable retry store
     * @param dataSourceProvider optional DataSource
     * @return the lifecycle repository
     * @throws IllegalStateException if the configured stores are not a built-in pair
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnExpression("${event-flow.retry.delivery.enabled:false} "
            + "or ${event-flow.retry.processing.durable-enabled:false}")
    public LifecycleRepository lifecycleRepository(EventStore eventStore,
                                                   EventRetryStore retryStore,
                                                   ObjectProvider<DataSource> dataSourceProvider) {
        if (eventStore instanceof JdbcEventStore jdbcEventStore
                && retryStore instanceof JdbcEventRetryStore jdbcRetryStore) {
            DataSource dataSource = dataSourceProvider.getIfAvailable();
            if (dataSource == null) {
                throw new IllegalStateException(
                        "JdbcLifecycleRepository requires a DataSource");
            }
            return new JdbcLifecycleRepository(dataSource, jdbcEventStore, jdbcRetryStore);
        }
        if (eventStore instanceof InMemoryEventStore
                && retryStore instanceof InMemoryEventRetryStore) {
            return new InMemoryLifecycleRepository(eventStore, retryStore);
        }
        throw new IllegalStateException(
                "Durable retry requires a built-in EventStore and EventRetryStore pair");
    }

    /**
     * Creates the shared scheduler support.
     *
     * @param retryStore          the durable retry store
     * @param lifecycleRepository the transactional lifecycle repository
     * @return the scheduler support
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnExpression("${event-flow.retry.delivery.enabled:false} "
            + "or ${event-flow.retry.processing.durable-enabled:false}")
    public RetrySchedulerSupport retrySchedulerSupport(EventRetryStore retryStore,
                                                       LifecycleRepository lifecycleRepository) {
        return new RetrySchedulerSupport(retryStore, lifecycleRepository,
                "retry-scheduler-" + UUID.randomUUID());
    }

    /**
     * Creates the delivery retry scheduler.
     *
     * @param support         shared scheduler support
     * @param eventStore      the event store holding payloads
     * @param publisherProvider the publisher used for re-publishing
     * @return the delivery retry scheduler
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.retry.delivery", name = "enabled",
            havingValue = "true")
    public DeliveryRetryScheduler deliveryRetryScheduler(
            RetrySchedulerSupport support,
            EventStore eventStore,
            ObjectProvider<EventPublisher> publisherProvider) {
        EventFlowProperties.DeliveryRetryConfig delivery = properties.getRetry().getDelivery();
        log.info("Creating DeliveryRetryScheduler: interval={}, ackTimeout={}, maxRetries={}, batchSize={}",
                delivery.getInterval(), delivery.getAckTimeout(),
                delivery.getMaxRetries(), delivery.getBatchSize());
        return new DeliveryRetryScheduler(support, eventStore, requirePublisher(publisherProvider),
                new DeliveryRetryOptions(requireService(), delivery.getInterval(),
                        delivery.getAckTimeout(), delivery.getMaxRetries(), delivery.getBatchSize()));
    }

    /**
     * Creates the durable processing retry scheduler.
     *
     * @param support           shared scheduler support
     * @param eventStore        the event store holding payloads
     * @param publisherProvider the publisher used for re-publishing
     * @return the processing retry scheduler
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.retry.processing", name = "durable-enabled",
            havingValue = "true")
    public ProcessingRetryScheduler processingRetryScheduler(
            RetrySchedulerSupport support,
            EventStore eventStore,
            ObjectProvider<EventPublisher> publisherProvider) {
        EventFlowProperties.ProcessingRetryConfig processing = properties.getRetry().getProcessing();
        EventFlowProperties.DefaultRetryPolicyConfig policy = processing.getDefaultPolicy();
        log.info("Creating ProcessingRetryScheduler: interval={}, ackTimeout={}, maxRetries={}, batchSize={}",
                processing.getDurableInterval(), processing.getDurableAckTimeout(),
                processing.getDurableMaxRetries(), processing.getDurableBatchSize());
        return new ProcessingRetryScheduler(support, eventStore, requirePublisher(publisherProvider),
                new ProcessingRetryOptions(requireService(), processing.getDurableInterval(),
                        processing.getDurableAckTimeout(), policy.getInitialDelay(),
                        policy.getMultiplier(), policy.getJitter(), policy.getMaxDelay(),
                        processing.getDurableMaxRetries(), processing.getDurableBatchSize()));
    }

    /**
     * Creates the watchdog for missing acknowledgments.
     *
     * @param eventStore the event store to scan
     * @param retryStore the durable retry store
     * @return the retry watchdog
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.retry.delivery", name = "enabled",
            havingValue = "true")
    public RetryWatchdog retryWatchdog(EventStore eventStore, EventRetryStore retryStore) {
        EventFlowProperties.DeliveryRetryConfig delivery = properties.getRetry().getDelivery();
        EventFlowProperties.ProcessingRetryConfig processing = properties.getRetry().getProcessing();
        return new RetryWatchdog(eventStore, retryStore,
                new RetryWatchdogOptions(requireService(), delivery.getInterval(),
                        delivery.getAckTimeout(), processing.getDurableAckTimeout(),
                        delivery.getBatchSize(), processing.isDurableEnabled()));
    }

    /**
     * Creates the ack handler that updates event lifecycle status upon receiving
     * {@code SuccessAck} / {@code FailureAck} acknowledgments.
     * <p>
     * This bean implements {@code EventSubscriber} and is automatically discovered
     * by the {@code SpringEventSubscriberRegistry}.
     *
     * @param eventStore          the event store to update
     * @param repositoryProvider  optional transactional lifecycle repository
     * @param retryStoreProvider  optional durable retry store
     * @return the ack handler
     * @throws IllegalStateException if service-name is not configured
     */
    @Bean
    @ConditionalOnMissingBean
    public AckHandler ackHandler(EventStore eventStore,
                                 ObjectProvider<LifecycleRepository> repositoryProvider,
                                 ObjectProvider<EventRetryStore> retryStoreProvider) {
        String service = requireService();
        LifecycleRepository repository = repositoryProvider.getIfAvailable();
        EventRetryStore retryStore = retryStoreProvider.getIfAvailable();
        log.info("Creating AckHandler with service: {}", service);
        if (repository != null && retryStore != null) {
            EventFlowProperties.ProcessingRetryConfig processing = properties.getRetry().getProcessing();
            EventFlowProperties.DefaultRetryPolicyConfig policy = processing.getDefaultPolicy();
            return new AckHandler(repository, retryStore, service,
                    new AckHandlerOptions(processing.isDurableEnabled(),
                            policy.getInitialDelay(), policy.getMaxDelay(),
                            processing.getDurableMaxRetries()));
        }
        return new AckHandler(eventStore, service);
    }

    /**
     * Creates the event retry scheduler.
     * <p>
     * Periodically scans for failed events (FAILED status) and stuck events
     * (PUBLISHED status) belonging to this service and re-publishes them.
     *
     * @param eventStore     the event store to scan
     * @param eventPublisher the publisher for re-publishing events
     * @return the retry scheduler
     * @throws IllegalStateException if service-name is not configured
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle.retry", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    @ConditionalOnExpression("!${event-flow.retry.delivery.enabled:false} "
            + "and !${event-flow.retry.processing.durable-enabled:false}")
    public EventRetryScheduler eventRetryScheduler(EventStore eventStore, EventPublisher eventPublisher) {
        LifecyclePublisherConfig.RetryTrackingConfig retry = properties.getPublisher().getLifecycle().getRetry();
        String service = properties.getPublisher().getLifecycle().getServiceName();
        if (StringUtils.isEmpty(service)) {
            throw new IllegalStateException(
                    "event-flow.publisher.lifecycle.service-name must be configured when lifecycle retry is enabled"
            );
        }
        log.info("Creating EventRetryScheduler: interval={}, minAge={}, maxRetries={}, batchSize={}, service={}",
                retry.getRetryInterval(), retry.getMinAge(), retry.getMaxRetries(), retry.getBatchSize(), service);
        return new EventRetryScheduler(
                eventStore,
                eventPublisher,
                retry.getRetryInterval(),
                retry.getMinAge(),
                retry.getMaxRetries(),
                retry.getBatchSize(),
                service
        );
    }

    /**
     * Creates the event cleanup scheduler.
     * <p>
     * Periodically deletes old terminal events (HANDLED, UNDEFINED) from
     * the event store to prevent unbounded growth.
     *
     * @param eventStore         the event store to clean up
     * @param retryStoreProvider optional durable retry store
     * @return the cleanup scheduler
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle.cleanup", name = "enabled",
            havingValue = "true")
    public EventCleanupScheduler eventCleanupScheduler(
            EventStore eventStore,
            ObjectProvider<EventRetryStore> retryStoreProvider) {
        LifecyclePublisherConfig.CleanupConfig cleanup = properties.getPublisher().getLifecycle().getCleanup();
        log.info("Creating EventCleanupScheduler: interval={}, maxAge={}, batchSize={}, pause={}, "
                        + "deadLetterMaxAge={}",
                cleanup.getInterval(), cleanup.getMaxAge(), cleanup.getBatchSize(),
                cleanup.getPauseBetweenBatches(), cleanup.getDeadLetterMaxAge());
        EventRetryStore retryStore = retryStoreProvider.getIfAvailable();
        if (retryStore != null) {
            return new EventCleanupScheduler(
                    eventStore,
                    cleanup.getInterval(),
                    cleanup.getMaxAge(),
                    cleanup.getBatchSize(),
                    cleanup.getPauseBetweenBatches(),
                    retryStore,
                    cleanup.getDeadLetterMaxAge()
            );
        }
        return new EventCleanupScheduler(
                eventStore,
                cleanup.getInterval(),
                cleanup.getMaxAge(),
                cleanup.getBatchSize(),
                cleanup.getPauseBetweenBatches()
        );
    }

    private String requireService() {
        String service = properties.getPublisher().getLifecycle().getServiceName();
        if (StringUtils.isEmpty(service)) {
            throw new IllegalStateException(
                    "event-flow.publisher.lifecycle.service-name must be configured when lifecycle tracking is enabled"
            );
        }
        return service;
    }

    private EventPublisher requirePublisher(ObjectProvider<EventPublisher> publisherProvider) {
        EventPublisher publisher = publisherProvider.getIfAvailable();
        if (publisher == null) {
            throw new IllegalStateException(
                    "Durable retry requires an EventPublisher bean; enable event-flow.publisher");
        }
        return publisher;
    }
}
