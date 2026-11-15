package io.github.vovten.eventflow.autoconfig.config;

import com.zaxxer.hikari.HikariDataSource;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

/**
 * Regression tests for the after-commit pool starvation (D-1): lifecycle JdbcEventStore
 * access inside {@code afterCommit} must reuse the transaction-bound connection instead
 * of requesting a second connection from the pool.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("Lifecycle DataSource Reuse Tests")
class LifecycleDataSourceReuseTest {

    @Test
    @DisplayName("DEFECT: afterCommit store access reuses the one connection held by the transaction")
    void shouldReuseTransactionBoundConnectionInAfterCommit() {
        HikariDataSource pool = new HikariDataSource();
        pool.setJdbcUrl("jdbc:h2:mem:lifecycle-reuse-" + UUID.randomUUID());
        pool.setUsername("sa");
        pool.setPassword("");
        pool.setMaximumPoolSize(1);
        pool.setConnectionTimeout(250);

        try (AnnotationConfigApplicationContext ctx = lifecycleContext(pool)) {
            EventStore store = ctx.getBean(EventStore.class);
            TransactionTemplate transaction = new TransactionTemplate(
                    new DataSourceTransactionManager(pool));

            UUID eventId = UUID.randomUUID();
            AtomicReference<Optional<StoredEvent>> found = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();

            transaction.executeWithoutResult(status -> {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive())
                        .as("the transaction must hold its connection")
                        .isTrue();
                TransactionSynchronizationManager.registerSynchronization(
                        new TransactionSynchronization() {
                            @Override
                            public void afterCommit() {
                                try {
                                    found.set(store.findById(eventId));
                                } catch (RuntimeException e) {
                                    failure.set(e);
                                }
                            }
                        });
            });

            assertThat(failure.get())
                    .as("afterCommit must not request a second connection from a one-connection pool")
                    .isNull();
            assertThat(found.get()).isNotNull();
            assertThat(found.get()).isEmpty();
        } finally {
            pool.close();
        }
    }

    private static AnnotationConfigApplicationContext lifecycleContext(DataSource dataSource) {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        addInlinedPropertiesToEnvironment(ctx,
                "event-flow.publisher.lifecycle.enabled=true");
        ctx.registerBean("dataSource", DataSource.class, () -> dataSource);
        ctx.registerBean("properties", EventFlowProperties.class, () -> {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().setEnabled(true);
            properties.getPublisher().getLifecycle().setEnabled(true);
            properties.getPublisher().getLifecycle().setServiceName("test-service");
            properties.getPublisher().getLifecycle().getStore().setAutoInitSchema(true);
            return properties;
        });
        ctx.register(LifecycleConfiguration.class);
        ctx.refresh();
        return ctx;
    }
}
