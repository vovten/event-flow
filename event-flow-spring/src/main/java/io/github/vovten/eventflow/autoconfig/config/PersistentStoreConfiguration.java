package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.event.lifecycle.AckHandler;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.PersistentEventPublisher;
import io.github.vovten.eventflow.publisher.EventRetryScheduler;
import io.github.vovten.eventflow.store.EventStore;
import io.github.vovten.eventflow.store.JdbcEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Auto-configuration for persistent event storage and lifecycle tracking.
 * <p>
 * Provides the following beans when {@code event-flow.publisher.persistent.enabled=true}:
 * <ul>
 *   <li>{@link EventStore} — JDBC-backed event store ({@link JdbcEventStore})</li>
 *   <li>{@link AckHandler} — processes lifecycle ack events (SuccessAck/FailureAck)</li>
 *   <li>{@link EventRetryScheduler} — periodically retries failed events</li>
 * </ul>
 * <p>
 * The {@link EventPublisher} from {@link PublisherConfiguration} is automatically
 * wrapped in a {@link PersistentEventPublisher} when persistent storage is enabled.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(DataSource.class)
@ConditionalOnProperty(prefix = "event-flow.publisher.persistent", name = "enabled", havingValue = "true")
public class PersistentStoreConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PersistentStoreConfiguration.class);

    private final EventFlowProperties properties;

    public PersistentStoreConfiguration(EventFlowProperties properties) {
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
        log.info("Creating JdbcEventStore");
        return new JdbcEventStore(dataSource);
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
        String service = properties.getPublisher().getPersistent().getService();
        log.info("Creating AckHandler with service: {}", service.isEmpty() ? "none" : service);
        return new AckHandler(eventStore, service);
    }

    /**
     * Creates the event retry scheduler.
     * <p>
     * Periodically scans for failed events (PUBLISH_FAILED, HANDLE_FAILED)
     * and re-publishes them.
     *
     * @param eventStore     the event store to scan
     * @param eventPublisher the publisher for re-publishing events
     * @return the retry scheduler
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event-flow.publisher.persistent", name = "retry-enabled",
            havingValue = "true", matchIfMissing = true)
    public EventRetryScheduler eventRetryScheduler(
            EventStore eventStore,
            EventPublisher eventPublisher) {
        EventFlowProperties.PersistentPublisherConfig config = properties.getPublisher().getPersistent();
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
