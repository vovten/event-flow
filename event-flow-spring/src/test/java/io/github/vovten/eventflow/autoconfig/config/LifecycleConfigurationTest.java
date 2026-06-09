package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.lifecycle.EventCleanupScheduler;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.db.JdbcEventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

/**
 * Tests for {@link LifecycleConfiguration}.
 *
 * @since 1.3.0
 */
class LifecycleConfigurationTest {

    @Test
    @DisplayName("Should create JdbcEventStore when store.type is db and DataSource is available")
    void shouldCreateJdbcEventStore() {
        try (AnnotationConfigApplicationContext ctx = createContext(dbProps(), true)) {
            EventStore store = ctx.getBean(EventStore.class);
            assertThat(store).isInstanceOf(JdbcEventStore.class);
            assertThat(store.getType()).isEqualTo("db");
        }
    }

    @Test
    @DisplayName("Should create InMemoryEventStore when store.type is in-memory")
    void shouldCreateInMemoryEventStore() {
        try (AnnotationConfigApplicationContext ctx = createContext(inMemoryProps(), false)) {
            EventStore store = ctx.getBean(EventStore.class);
            assertThat(store).isInstanceOf(InMemoryEventStore.class);
            assertThat(store.getType()).isEqualTo("in-memory");
        }
    }

    @Test
    @DisplayName("Should throw IllegalStateException when store.type is db but no DataSource available")
    void shouldThrowWhenNoDataSource() {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        addInlinedPropertiesToEnvironment(ctx, "event-flow.publisher.lifecycle.enabled=true");
        ctx.registerBean("properties", EventFlowProperties.class, () -> {
            EventFlowProperties props = new EventFlowProperties();
            props.getPublisher().setEnabled(true);
            props.getPublisher().getLifecycle().setEnabled(true);
            props.getPublisher().getLifecycle().setServiceName("test-service");
            // store.type defaults to "db"
            return props;
        });
        ctx.register(LifecycleConfiguration.class);

        assertThatThrownBy(ctx::refresh)
                .isInstanceOf(BeanCreationException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .rootCause()
                .hasMessageContaining("no DataSource available");
    }

    @Test
    @DisplayName("Should return custom EventStore bean when store.type matches its type")
    void shouldReturnCustomEventStoreWhenTypeMatches() {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
            addInlinedPropertiesToEnvironment(ctx,
                    "event-flow.publisher.lifecycle.enabled=true",
                    "event-flow.publisher.lifecycle.retry.enabled=false",
                    "event-flow.publisher.lifecycle.store.type=custom");
            ctx.registerBean("properties", EventFlowProperties.class, () -> {
                EventFlowProperties props = new EventFlowProperties();
                props.getPublisher().setEnabled(true);
                props.getPublisher().getLifecycle().setEnabled(true);
                props.getPublisher().getLifecycle().setServiceName("test-service");
                props.getPublisher().getLifecycle().getStore().setType("custom");
                return props;
            });
            ctx.register(LifecycleConfiguration.class);
            ctx.register(CustomStoreConfig.class);
            ctx.refresh();

            EventStore store = ctx.getBean(EventStore.class);
            assertThat(store.getType()).isEqualTo("custom");
            assertThat(store).isInstanceOf(CustomEventStore.class);

            // The custom bean also exists by its own name
            assertThat(ctx.getBeanNamesForType(EventStore.class))
                    .contains("customEventStore");
        }
    }

    @Test
    @DisplayName("Should create EventCleanupScheduler when cleanup.enabled=true")
    void shouldCreateEventCleanupScheduler() {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
            addInlinedPropertiesToEnvironment(ctx,
                    "event-flow.publisher.lifecycle.enabled=true",
                    "event-flow.publisher.lifecycle.retry.enabled=false",
                    "event-flow.publisher.lifecycle.cleanup.enabled=true");
            ctx.registerBean("properties", EventFlowProperties.class,
                    () -> inMemoryCleanupProps(false, true));
            ctx.register(LifecycleConfiguration.class);
            ctx.refresh();

            assertThat(ctx.getBean(EventCleanupScheduler.class)).isNotNull();
        }
    }

    @Test
    @DisplayName("Should not create EventCleanupScheduler when cleanup.enabled=false")
    void shouldNotCreateEventCleanupScheduler() {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
            addInlinedPropertiesToEnvironment(ctx,
                    "event-flow.publisher.lifecycle.enabled=true",
                    "event-flow.publisher.lifecycle.retry.enabled=false",
                    "event-flow.publisher.lifecycle.cleanup.enabled=false");
            ctx.registerBean("properties", EventFlowProperties.class,
                    () -> inMemoryCleanupProps(false, false));
            ctx.register(LifecycleConfiguration.class);
            ctx.refresh();

            assertThatThrownBy(() -> ctx.getBean(EventCleanupScheduler.class));
        }
    }

    // -- helpers --

    private static AnnotationConfigApplicationContext createContext(EventFlowProperties props,
                                                                     boolean withDataSource) {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        addInlinedPropertiesToEnvironment(ctx,
                "event-flow.publisher.lifecycle.enabled=true",
                "event-flow.publisher.lifecycle.retry.enabled=false");
        if (withDataSource) {
            ctx.registerBean("dataSource", DataSource.class, LifecycleConfigurationTest::mockDataSource);
        }
        ctx.registerBean("properties", EventFlowProperties.class, () -> props);
        ctx.register(LifecycleConfiguration.class);
        ctx.refresh();
        return ctx;
    }

    private static EventFlowProperties dbProps() {
        EventFlowProperties props = new EventFlowProperties();
        props.getPublisher().setEnabled(true);
        props.getPublisher().getLifecycle().setEnabled(true);
        props.getPublisher().getLifecycle().setServiceName("test-service");
        // store.type defaults to "db"
        return props;
    }

    private static EventFlowProperties inMemoryProps() {
        EventFlowProperties props = new EventFlowProperties();
        props.getPublisher().setEnabled(true);
        props.getPublisher().getLifecycle().setEnabled(true);
        props.getPublisher().getLifecycle().setServiceName("test-service");
        props.getPublisher().getLifecycle().getStore().setType("in-memory");
        return props;
    }

    private static EventFlowProperties inMemoryCleanupProps(boolean retryEnabled, boolean cleanupEnabled) {
        EventFlowProperties props = new EventFlowProperties();
        props.getPublisher().setEnabled(true);
        props.getPublisher().getLifecycle().setEnabled(true);
        props.getPublisher().getLifecycle().setServiceName("test-service");
        props.getPublisher().getLifecycle().getStore().setType("in-memory");
        props.getPublisher().getLifecycle().getRetry().setEnabled(retryEnabled);
        props.getPublisher().getLifecycle().getCleanup().setEnabled(cleanupEnabled);
        return props;
    }

    private static DataSource mockDataSource() {
        try {
            DataSource ds = mock(DataSource.class);
            Connection conn = mock(Connection.class);
            DatabaseMetaData meta = mock(DatabaseMetaData.class);
            Statement stmt = mock(Statement.class);
            ResultSet emptyRs = mock(ResultSet.class);

            when(ds.getConnection()).thenReturn(conn);
            when(conn.getMetaData()).thenReturn(meta);
            when(meta.getDatabaseProductName()).thenReturn("H2");
            when(conn.createStatement()).thenReturn(stmt);
            when(stmt.execute(anyString())).thenReturn(true);

            when(meta.getTables(null, null, "event_store", null)).thenReturn(emptyRs);
            when(meta.getTables(null, null, "EVENT_STORE", null)).thenReturn(emptyRs);
            when(meta.getTables(null, null, "EVENT_STORE".toLowerCase(), null)).thenReturn(emptyRs);
            when(emptyRs.next()).thenReturn(false);

            return ds;
        } catch (Exception e) {
            throw new RuntimeException("Failed to mock DataSource", e);
        }
    }

    @Configuration
    static class CustomStoreConfig {
        @Bean
        public EventStore customEventStore() {
            return new CustomEventStore();
        }
    }

    static class CustomEventStore implements EventStore {
        @Override
        public String getType() {
            return "custom";
        }

        @Override
        public void save(StoredEvent event) {
        }

        @Override
        public void updateStatus(UUID eventId, EventStatus status, String errorDetails) {
        }

        @Override
        public List<StoredEvent> findByStatus(EventStatus status, Instant before) {
            return Collections.emptyList();
        }

        @Override
        public Optional<StoredEvent> findById(UUID eventId) {
            return Optional.empty();
        }

        @Override
        public List<StoredEvent> findByStatuses(List<EventStatus> statuses, Instant before, int batchSize) {
            return Collections.emptyList();
        }

        @Override
        public int deleteByStatuses(List<EventStatus> statuses, Instant before, int batchSize) {
            return 0;
        }
    }
}
