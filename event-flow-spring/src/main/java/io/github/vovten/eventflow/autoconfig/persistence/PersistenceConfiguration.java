package io.github.vovten.eventflow.autoconfig.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.publisher.persistence.EventRepository;
import io.github.vovten.eventflow.publisher.persistence.jdbc.JdbcEventRepositoryBuilder;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Auto-configuration for event persistence infrastructure.
 * <p>
 * Creates DataSource, EventRepository, and EventSerializer beans
 * for the transactional outbox pattern.
 * <p>
 * Note: The actual wrapping of EventPublisher with PersistentEventPublisher
 * is done in PublisherConfiguration.
 * <p>
 * Configuration example:
 * <pre>{@code
 * event-flow:
 *   enabled: true
 *   publisher:
 *     enabled: true
 *     persistence:
 *       enabled: true
 *       jdbc:
 *         url: jdbc:postgresql://localhost:5432/events
 *         schema: events
 *         table-name: event_outbox
 *         username: user
 *         password: pass
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow.publisher.persistence", name = "enabled", havingValue = "true")
public class PersistenceConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PersistenceConfiguration.class);

    /**
     * Creates HikariCP DataSource from properties.
     */
    @Bean
    @ConditionalOnMissingBean(name = "persistenceDataSource")
    @ConditionalOnProperty(prefix = "event-flow.publisher.persistence.jdbc", name = "url")
    public DataSource persistenceDataSource(EventFlowProperties properties) {
        EventFlowProperties.JdbcConfig jdbc = properties.getPublisher().getPersistence().getJdbc();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbc.getUrl());
        config.setUsername(jdbc.getUsername());
        config.setPassword(jdbc.getPassword());
        config.setPoolName("event-flow-persistence-pool");

        if (jdbc.getMaximumPoolSize() != null) {
            config.setMaximumPoolSize(jdbc.getMaximumPoolSize());
        }
        if (jdbc.getMinimumIdle() != null) {
            config.setMinimumIdle(jdbc.getMinimumIdle());
        }

        log.info("Creating HikariCP DataSource for event persistence: {}", jdbc.getUrl());
        return new HikariDataSource(config);
    }

    /**
     * Creates JDBC event repository.
     */
    @Bean
    @ConditionalOnMissingBean(EventRepository.class)
    public EventRepository eventRepository(DataSource dataSource, EventFlowProperties properties) {
        EventFlowProperties.JdbcConfig jdbc = properties.getPublisher().getPersistence().getJdbc();

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