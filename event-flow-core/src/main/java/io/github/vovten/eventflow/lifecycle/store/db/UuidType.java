package io.github.vovten.eventflow.lifecycle.store.db;

/**
 * UUID column storage strategy for relational databases.
 * <p>
 * {@link #NATIVE} uses the database-native {@code UUID} type (e.g. PostgreSQL, H2).
 * {@link #BINARY} uses {@code BINARY(16)} with byte[] JDBC binding (e.g. MySQL, Oracle, SQL Server).
 *
 * @author Vladimir Aleshkov
 * @since 1.3.2
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

    /**
     * Derives the UUID storage strategy from a database dialect.
     * <p>
     * PostgreSQL and H2 use native UUID; all others use BINARY(16).
     *
     * @param dialect the database dialect
     * @return the corresponding UUID type
     */
    public static UuidType fromDialect(DatabaseDialect dialect) {
        return switch (dialect) {
            case POSTGRESQL, H2 -> NATIVE;
            default -> BINARY;
        };
    }
}
