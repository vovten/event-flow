package io.github.vovten.eventflow.publisher.persistence.jdbc;

import javax.sql.DataSource;

/**
 * Builder for {@link JdbcEventRepository}.
 * <p>
 * Usage example:
 * <pre>{@code
 * DataSource dataSource = ...;
 * EventRepository repository = JdbcEventRepositoryBuilder.builder()
 *         .dataSource(dataSource)
 *         .schema("events")
 *         .tableName("event_outbox")
 *         .createTableIfNotExists(true)
 *         .build();
 * }</pre>
 */
public class JdbcEventRepositoryBuilder {

    private DataSource dataSource;
    private String schema = "public";
    private String tableName = "event_outbox";
    private boolean createTableIfNotExists = true;

    private JdbcEventRepositoryBuilder() {
    }

    public static JdbcEventRepositoryBuilder builder() {
        return new JdbcEventRepositoryBuilder();
    }

    public JdbcEventRepositoryBuilder dataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public JdbcEventRepositoryBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }

    public JdbcEventRepositoryBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public JdbcEventRepositoryBuilder createTableIfNotExists(boolean create) {
        this.createTableIfNotExists = create;
        return this;
    }

    public JdbcEventRepository build() {
        if (dataSource == null) {
            throw new IllegalArgumentException("DataSource is required");
        }
        return new JdbcEventRepository(dataSource, schema, tableName, createTableIfNotExists);
    }
}