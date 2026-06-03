package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.lifecycle.AckHandler;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.EventRetryScheduler;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.JdbcEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Auto-configuration for lifecycle-aware event publishing and lifecycle tracking.
 * <p>
 * Provides the following beans when {@code event-flow.publisher.lifecycle.enabled=true}:
 * <ul>
 *   <li>{@link EventStore} — JDBC-backed event store ({@link JdbcEventStore})</li>
 *   <li>{@link AckHandler} — processes lifecycle ack events (SuccessAck/FailureAck)</li>
 *   <li>{@link EventRetryScheduler} — periodically retries failed events</li>
 * </ul>
 * <p>
 * The {@link EventPublisher} from {@link PublisherConfiguration} is automatically
 * wrapped in an {@link io.github.vovten.eventflow.lifecycle.EventLifecyclePublisher}
 * when lifecycle-aware publishing is enabled.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(DataSource.class)
@ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle", name = "enabled", havingValue = "true")
public class LifecycleConfiguration {

    private static final Logger log = LoggerFactory.getLogger(LifecycleConfiguration.class);

    private final EventFlowProperties properties;

    public LifecycleConfiguration(EventFlowProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates the JDBC-backed event store.
     *
     * @param dataSource the JDBC DataSource (provided by Spring Boot auto-configuration)
     * @return the event store instance
     */
    @Bean
    @ConditionalOnMissingBean
    public EventStore eventStore(DataSource dataSource) {
        EventFlowProperties.LifecyclePublisherConfig config = properties.getPublisher().getLifecycle();
        log.info("Creating JdbcEventStore with table name: {}, auto-init-schema: {}",
                config.getTableName(), config.isAutoInitSchema());
        return new JdbcEventStore(dataSource, config.getTableName(), config.isAutoInitSchema());
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
     */
    @Bean
    @ConditionalOnMissingBean
    public AckHandler ackHandler(EventStore eventStore) {
        String service = properties.getPublisher().getLifecycle().getServiceName();
        log.info("Creating AckHandler with service: {}", service.isEmpty() ? "none" : service);
        return new AckHandler(eventStore, service);
    }

    /**
     * Creates the event retry scheduler.
     * <p>
     * Periodically scans for failed events (FAILED status) and re-publishes them.
     *
     * @param eventStore     the event store to scan
     * @param eventPublisher the publisher for re-publishing events
     * @return the retry scheduler
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.publisher.lifecycle", name = "retry-enabled",
            havingValue = "true", matchIfMissing = true)
    public EventRetryScheduler eventRetryScheduler(
            EventStore eventStore,
            EventPublisher eventPublisher) {
        EventFlowProperties.LifecyclePublisherConfig config = properties.getPublisher().getLifecycle();
        log.info("Creating EventRetryScheduler: interval={}, minAge={}, maxRetries={}",
                config.getRetryInterval(), config.getMinAge(), config.getMaxRetries());
        return new EventRetryScheduler(
                eventStore,
                eventPublisher,
                config.getRetryInterval(),
                config.getMinAge(),
                config.getMaxRetries()
        );
    }
}
