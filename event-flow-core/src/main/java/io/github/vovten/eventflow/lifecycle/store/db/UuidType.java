package io.github.vovten.eventflow.lifecycle.store.db;

/**
 * UUID column storage strategy for relational databases.
 * <p>
 * {@link #NATIVE} uses the database-native {@code UUID} type (e.g. PostgreSQL, H2).
 * {@link #BINARY} uses {@code BINARY(16)} with byte[] JDBC binding (e.g. MySQL, Oracle, SQL Server).
 * <p>
 * UUID strategy is configured per dialect via {@code SqlDialect.uuidType()}.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public enum UuidType {

    /**
     * Database-native UUID type (PostgreSQL, H2).
     */
    NATIVE,

    /**
     * Binary(16) byte array for databases without native UUID support (MySQL, Oracle, etc.).
     */
    BINARY;
}
