package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties.LifecyclePublisherConfig;
import io.github.vovten.eventflow.lifecycle.AckHandler;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.EventRetryScheduler;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.EventStoreRegistry;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.JdbcEventStore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.List;

/**
 * Auto-configuration for lifecycle-aware event publishing and lifecycle tracking.
 * <p>
 * Provides the following beans when {@code event-flow.publisher.lifecycle.enabled=true}:
 * <ul>
 *   <li>{@link EventStore} — resolved from {@link EventStoreRegistry} by {@code store.type}</li>
 *   <li>{@link AckHandler} — processes lifecycle ack events (SuccessAck/FailureAck)</li>
 *   <li>{@link EventRetryScheduler} — periodically retries failed events</li>
 * </ul>
 * <p>
 * Built-in store types:
 * <ul>
 *   <li>{@code "db"} — {@link JdbcEventStore} (requires DataSource, schema auto-init)</li>
 *   <li>{@code "in-memory"} — {@link InMemoryEventStore} (non-persistent, for testing)</li>
 * </ul>
 * <p>
 * Custom store implementations can be provided by creating an {@link EventStore} bean
 * with a custom {@link EventStore#getType()} value. The registry automatically discovers
 * and registers custom stores. Select the store type via {@code store.type} in configuration.
 * <p>
 * The {@link EventPublisher} from {@link PublisherConfiguration} is automatically
 * wrapped in an {@link io.github.vovten.eventflow.lifecycle.EventLifecyclePublisher}
 * when lifecycle-aware publishing is enabled.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle", name = "enabled", havingValue = "true")
public class LifecycleConfiguration {

    private static final Logger log = LoggerFactory.getLogger(LifecycleConfiguration.class);

    private final EventFlowProperties properties;

    public LifecycleConfiguration(EventFlowProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates the {@link EventStoreRegistry} that holds all registered store
     * implementations and resolves the active one by configured type.
     *
     * @return the event store registry
     */
    @Bean
    @ConditionalOnMissingBean
    public EventStoreRegistry eventStoreRegistry() {
        return new EventStoreRegistry();
    }

    /**
     * Registers the built-in {@link JdbcEventStore} (type {@code "db"}).
     * <p>
     * Only registered when {@code store.type} is {@code "db"} (or default)
     * and a {@link DataSource} is available.
     *
     * @param registry   the event store registry to register into
     * @param dataSource the JDBC DataSource for database access
     * @return marker object (for dependency ordering)
     */
    @Bean
    @ConditionalOnClass(DataSource.class)
    @ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle.store", name = "type",
            havingValue = "db", matchIfMissing = true)
    public Object registerJdbcStore(EventStoreRegistry registry, DataSource dataSource) {
        LifecyclePublisherConfig.StoreConfig cfg = properties.getPublisher().getLifecycle().getStore();
        registry.register(new JdbcEventStore(dataSource, cfg.getTableName(), cfg.isAutoInitSchema()));
        log.info("Registered JdbcEventStore (type='db', table={}, autoInit={})",
                cfg.getTableName(), cfg.isAutoInitSchema());
        return new Object();
    }

    /**
     * Registers the built-in {@link InMemoryEventStore} (type {@code "in-memory"}).
     * <p>
     * Only registered when {@code store.type} is explicitly set to {@code "in-memory"}.
     *
     * @param registry the event store registry to register into
     * @return marker object (for dependency ordering)
     */
    @Bean
    @ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle.store", name = "type",
            havingValue = "in-memory")
    public Object registerInMemoryStore(EventStoreRegistry registry) {
        registry.register(new InMemoryEventStore());
        log.warn("Registered InMemoryEventStore (type='in-memory') — event data is NOT persisted across restarts");
        return new Object();
    }

    /**
     * Discovers and registers custom {@link EventStore} beans provided by the user.
     * <p>
     * Any {@link EventStore} bean not created by this configuration (e.g., user-defined
     * {@code @Component} or {@code @Bean}) is automatically registered in the registry
     * under its {@link EventStore#getType()} identifier.
     *
     * @param registry       the event store registry to register into
     * @param customStores   user-defined EventStore beans (auto-injected)
     * @return marker object (for dependency ordering)
     */
    @Bean
    public Object registerCustomStores(EventStoreRegistry registry, List<EventStore> customStores) {
        for (EventStore store : customStores) {
            registry.register(store);
            log.info("Registered custom EventStore: type='{}'", store.getType());
        }
        return new Object();
    }

    /**
     * Resolves the active {@link EventStore} from the registry based on
     * the configured {@code store.type}.
     * <p>
     * Uses {@link EventStoreRegistry#resolve(String)} to look up the store.
     *
     * @param registry the event store registry
     * @return the resolved event store
     * @throws IllegalArgumentException if no store is registered for the configured type
     */
    @Bean
    @ConditionalOnMissingBean
    public EventStore eventStore(EventStoreRegistry registry) {
        String type = properties.getPublisher().getLifecycle().getStore().getType();
        EventStore store = registry.resolve(type);
        log.info("Resolved EventStore: type='{}'", store.getType());
        return store;
    }

    /**
     * Creates the ack handler that updates event lifecycle status upon receiving
     * {@code SuccessAck} / {@code FailureAck} acknowledgments.
     * <p>
     * This bean implements {@code EventSubscriber} and is automatically discovered
     * by the {@code SpringEventSubscriberRegistry}.
     *
     * @param eventStore the event store to update
     * @return the ack handler
     * @throws IllegalStateException if service-name is not configured
     */
    @Bean
    @ConditionalOnMissingBean
    public AckHandler ackHandler(EventStore eventStore) {
        String service = properties.getPublisher().getLifecycle().getServiceName();
        if (StringUtils.isEmpty(service)) {
            throw new IllegalStateException(
                    "event-flow.publisher.lifecycle.service-name must be configured when lifecycle tracking is enabled"
            );
        }
        log.info("Creating AckHandler with service: {}", service);
        return new AckHandler(eventStore, service);
    }

    /**
     * Creates the event retry scheduler.
     * <p>
     * Periodically scans for failed events (FAILED status) and stuck events
     * (PUBLISHED status) and re-publishes them.
     *
     * @param eventStore     the event store to scan
     * @param eventPublisher the publisher for re-publishing events
     * @return the retry scheduler
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle.retry", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public EventRetryScheduler eventRetryScheduler(EventStore eventStore, EventPublisher eventPublisher) {
        LifecyclePublisherConfig.RetryTrackingConfig retry = properties.getPublisher().getLifecycle().getRetry();
        log.info("Creating EventRetryScheduler: interval={}, minAge={}, maxRetries={}",
                retry.getRetryInterval(), retry.getMinAge(), retry.getMaxRetries());
        return new EventRetryScheduler(
                eventStore,
                eventPublisher,
                retry.getRetryInterval(),
                retry.getMinAge(),
                retry.getMaxRetries()
        );
    }
}
