package io.github.vovten.eventflow.autoconfig.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.persistence.EventRepository;
import io.github.vovten.eventflow.publisher.persistence.PersistentEventPublisher;
import io.github.vovten.eventflow.publisher.persistence.jdbc.JdbcEventRepository;
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
 * Auto-configuration for persistent event publisher.
 * <p>
 * Enables event persistence to database before publishing, implementing
 * the transactional outbox pattern.
 * <p>
 * Configuration example:
 * <pre>{@code
 * event-flow:
 *   publisher:
 *     persistence:
 *       enabled: true
 *       jdbc:
 *         url: jdbc:postgresql://localhost:5432/events
 *         table-name: public.event_outbox
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

    @Bean
    @ConditionalOnMissingBean(name = "eventFlowDataSource")
    @ConditionalOnProperty(prefix = "event-flow.publisher.persistence.jdbc", name = "url")
    public DataSource eventFlowDataSource(EventFlowProperties properties) {
        EventFlowProperties.JdbcConfig jdbc = properties.getPublisher().getPersistence().getJdbc();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbc.getUrl());
        config.setUsername(jdbc.getUsername());
        config.setPassword(jdbc.getPassword());
        config.setPoolName("event-flow-pool");

        if (jdbc.getMaximumPoolSize() != null) {
            config.setMaximumPoolSize(jdbc.getMaximumPoolSize());
        }
        if (jdbc.getMinimumIdle() != null) {
            config.setMinimumIdle(jdbc.getMinimumIdle());
        }

        log.info("Creating HikariCP DataSource for: {}", jdbc.getUrl());
        return new HikariDataSource(config);
    }

    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnMissingBean(EventRepository.class)
    public EventRepository eventRepository(DataSource dataSource, EventFlowProperties properties) {
        EventFlowProperties.JdbcConfig jdbc = properties.getPublisher().getPersistence().getJdbc();

        log.info("Creating JdbcEventRepository with table={}", jdbc.getTableName());

        return JdbcEventRepositoryBuilder.builder()
                .dataSource(dataSource)
                .tableName(jdbc.getTableName())
                .createTableIfNotExists(true)
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "persistenceEventSerializer")
    public EventSerializer persistenceEventSerializer() {
        return new JsonEventSerializer();
    }

    @Bean
    @ConditionalOnBean(EventPublisher.class)
    @ConditionalOnMissingBean(name = "persistentEventPublisher")
    public EventPublisher persistentEventPublisher(
            EventPublisher delegate,
            EventRepository repository,
            EventSerializer serializer) {

        log.info("Creating PersistentEventPublisher wrapping delegate publisher");

        return new PersistentEventPublisher(delegate, repository, serializer);
    }
}