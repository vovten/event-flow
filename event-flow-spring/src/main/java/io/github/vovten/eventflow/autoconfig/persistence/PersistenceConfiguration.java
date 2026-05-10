package io.github.vovten.eventflow.autoconfig.persistence;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.publisher.persistence.EventRepository;
import io.github.vovten.eventflow.publisher.persistence.jdbc.JdbcEventRepositoryBuilder;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Auto-configuration for event persistence infrastructure.
 * <p>
 * Creates EventRepository and EventSerializer beans for the transactional outbox pattern.
 * Uses the existing application DataSource (no separate pool created).
 * <p>
 * Configuration example:
 * <pre>{@code
 * event-flow:
 *   enabled: true
 *   publisher:
 *     enabled: true
 *     persistence:
 *       enabled: true
 *       schema: events
 *       table-name: event_outbox
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow.publisher.persistence", name = "enabled", havingValue = "true")
@ConditionalOnBean(DataSource.class)
public class PersistenceConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PersistenceConfiguration.class);

    /**
     * Creates JDBC event repository using the application DataSource.
     */
    @Bean
    @ConditionalOnMissingBean(EventRepository.class)
    public EventRepository eventRepository(DataSource dataSource, EventFlowProperties properties) {
        var jdbc = properties.getPublisher().getPersistence().getJdbc();

        log.info("Creating JdbcEventRepository with schema={}, table={}", jdbc.getSchema(), jdbc.getTableName());

        return JdbcEventRepositoryBuilder.builder()
                .dataSource(dataSource)
                .schema(jdbc.getSchema())
                .tableName(jdbc.getTableName())
                .createTableIfNotExists(true)
                .build();
    }

    /**
     * Creates event serializer for persistence.
     */
    @Bean("persistenceEventSerializer")
    @ConditionalOnMissingBean(name = "persistenceEventSerializer")
    public EventSerializer persistenceEventSerializer() {
        return new JsonEventSerializer();
    }
}