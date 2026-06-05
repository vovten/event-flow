package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties.LifecyclePublisherConfig;
import io.github.vovten.eventflow.lifecycle.AckHandler;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.EventRetryScheduler;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.JdbcEventStore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.List;

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
 *   <li>{@code "db"} — {@link JdbcEventStore} (requires DataSource, schema auto-init)</li>
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
