package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.LifecycleRepository;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.retry.RetryKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JDBC implementation of {@link LifecycleRepository}.
 * <p>
 * Each transition updates {@code event_store} and {@code event_retry} on one
 * connection inside a single transaction. A failure on either side rolls back
 * both.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class JdbcLifecycleRepository implements LifecycleRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcLifecycleRepository.class);

    private static final Set<EventStatus> PUBLISH_FAILURE_SOURCES =
            Set.of(EventStatus.NEW, EventStatus.FAILED);

    private static final Set<EventStatus> ACCEPTANCE_SOURCES =
            Set.of(EventStatus.NEW, EventStatus.PUBLISHED);

    private static final Set<EventStatus> PROCESSING_SOURCES =
            Set.of(EventStatus.NEW, EventStatus.PUBLISHED, EventStatus.ACCEPTED, EventStatus.FAILED);

    /**
     * Manual retry may resurrect any terminal or failed status, but never
     * an event that is already {@code NEW}: a second manual retry while the
     * retry is already scheduled must not increment {@code retry_count} again.
     */
    private static final Set<EventStatus> MANUAL_RETRY_SOURCES = Set.of(
            EventStatus.UNDEFINED, EventStatus.PUBLISHED, EventStatus.ACCEPTED,
            EventStatus.HANDLED, EventStatus.FAILED, EventStatus.DEAD);

    private final DataSource dataSource;
    private final JdbcEventStore eventStore;
    private final JdbcEventRetryStore retryStore;

    /**
     * Creates a repository.
     *
     * @param dataSource the DataSource used for transactions
     * @param eventStore the JDBC event store
     * @param retryStore the JDBC retry store
     */
    public JdbcLifecycleRepository(DataSource dataSource,
                                   JdbcEventStore eventStore,
                                   JdbcEventRetryStore retryStore) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.retryStore = Objects.requireNonNull(retryStore, "retryStore must not be null");
    }

    @Override
    public void onPublishFailed(UUID eventId, String service, RetryReason reason,
                                Instant nextAttemptAt, String errorType, String error) {
        inTransaction(conn -> {
            boolean applied = eventStore.transitionStatus(
                    conn, eventId, PUBLISH_FAILURE_SOURCES, EventStatus.FAILED, error);
            if (applied) {
                schedule(conn, eventId, service, RetryKind.DELIVERY, 1,
                        reason, nextAttemptAt, null, errorType, error);
            }
        });
    }

    @Override
    public void onAccepted(UUID eventId, int deliveryAttempt) {
        tryOnAccepted(eventId, deliveryAttempt);
    }

    @Override
    public boolean tryOnAccepted(UUID eventId, int deliveryAttempt) {
        AtomicBoolean processed = new AtomicBoolean(false);
        inTransaction(conn -> {
            boolean applied = eventStore.transitionStatus(
                    conn, eventId, ACCEPTANCE_SOURCES, EventStatus.ACCEPTED, null);
            // A delivery confirmation for a terminal event preserves the status
            // but must still clear the DELIVERY row. Only an unknown event makes
            // the ack unprocessable.
            if (applied || eventStore.findById(conn, eventId).isPresent()) {
                retryStore.complete(conn, eventId, RetryKind.DELIVERY);
                processed.set(true);
            }
        });
        return processed.get();
    }

    @Override
    public void onProcessingFailed(UUID eventId, String service, int attempt,
                                   boolean retryable, Instant nextAttemptAt,
                                   String policyName, String errorType, String error) {
        tryOnProcessingFailed(eventId, service, attempt, retryable, nextAttemptAt,
                policyName, errorType, error);
    }

    @Override
    public boolean tryOnProcessingFailed(UUID eventId, String service, int attempt,
                                         boolean retryable, Instant nextAttemptAt,
                                         String policyName, String errorType, String error) {
        AtomicBoolean applied = new AtomicBoolean(false);
        inTransaction(conn -> {
            EventStatus target = retryable ? EventStatus.FAILED : EventStatus.DEAD;
            boolean updated = eventStore.transitionStatus(
                    conn, eventId, PROCESSING_SOURCES, target, error);
            applied.set(updated);
            if (!updated) {
                return;
            }
            retryStore.complete(conn, eventId, RetryKind.DELIVERY);
            if (retryable) {
                schedule(conn, eventId, service, RetryKind.PROCESSING, attempt,
                        RetryReason.HANDLER_FAILED, nextAttemptAt, policyName, errorType, error);
            } else {
                deadOrSchedule(conn, eventId, service, RetryKind.PROCESSING,
                        RetryReason.HANDLER_FAILED, policyName, errorType, error);
            }
        });
        return applied.get();
    }

    @Override
    public void onProcessingDisabled(UUID eventId, String errorType, String error) {
        tryOnProcessingDisabled(eventId, errorType, error);
    }

    @Override
    public boolean tryOnProcessingDisabled(UUID eventId, String errorType, String error) {
        AtomicBoolean applied = new AtomicBoolean(false);
        inTransaction(conn -> {
            boolean updated = eventStore.transitionStatus(
                    conn, eventId, PROCESSING_SOURCES, EventStatus.FAILED, error);
            applied.set(updated);
            if (updated) {
                retryStore.complete(conn, eventId, RetryKind.DELIVERY);
            }
        });
        return applied.get();
    }

    @Override
    public void onHandled(UUID eventId, int attempt) {
        tryOnHandled(eventId, attempt);
    }

    @Override
    public boolean tryOnHandled(UUID eventId, int attempt) {
        AtomicBoolean applied = new AtomicBoolean(false);
        inTransaction(conn -> {
            boolean updated = eventStore.transitionStatus(
                    conn, eventId, PROCESSING_SOURCES, EventStatus.HANDLED, null);
            applied.set(updated);
            if (updated) {
                retryStore.complete(conn, eventId, RetryKind.DELIVERY);
                retryStore.complete(conn, eventId, RetryKind.PROCESSING);
            }
        });
        return applied.get();
    }

    @Override
    public void onNoHandler(UUID eventId, String service, int attempt,
                            String errorType, String error) {
        tryOnNoHandler(eventId, service, attempt, errorType, error);
    }

    @Override
    public boolean tryOnNoHandler(UUID eventId, String service, int attempt,
                                  String errorType, String error) {
        AtomicBoolean applied = new AtomicBoolean(false);
        inTransaction(conn -> {
            boolean updated = eventStore.transitionStatus(
                    conn, eventId, PROCESSING_SOURCES, EventStatus.DEAD, error);
            applied.set(updated);
            if (updated) {
                retryStore.complete(conn, eventId, RetryKind.DELIVERY);
                deadOrSchedule(conn, eventId, service, RetryKind.PROCESSING,
                        RetryReason.NO_HANDLER, null, errorType, error);
            }
        });
        return applied.get();
    }

    @Override
    public void onDeadLetter(UUID eventId, String service, RetryKind kind,
                             RetryReason reason, String errorType, String error) {
        tryOnDeadLetter(eventId, service, kind, reason, errorType, error);
    }

    @Override
    public boolean tryOnDeadLetter(UUID eventId, String service, RetryKind kind,
                                   RetryReason reason, String errorType, String error) {
        AtomicBoolean applied = new AtomicBoolean(false);
        inTransaction(conn -> {
            boolean updated = eventStore.transitionStatus(
                    conn, eventId, PROCESSING_SOURCES, EventStatus.DEAD, error);
            applied.set(updated);
            if (updated) {
                deadOrSchedule(conn, eventId, service, kind, reason, null, errorType, error);
            }
        });
        return applied.get();
    }

    @Override
    public void markForRetry(UUID eventId, String service) {
        tryMarkForRetry(eventId, service);
    }

    @Override
    public boolean tryMarkForRetry(UUID eventId, String service) {
        AtomicBoolean applied = new AtomicBoolean(false);
        inTransaction(conn -> {
            boolean updated = eventStore.transitionStatus(
                    conn, eventId, MANUAL_RETRY_SOURCES, EventStatus.NEW, null);
            applied.set(updated);
            if (!updated) {
                return;
            }
            RetryKind kind = resolveKind(conn, eventId);
            Optional<RetryTask> existing = retryStore.find(conn, eventId, kind);
            String policyName = existing.map(RetryTask::policyName).orElse(null);
            schedule(conn, eventId, service, kind, 1,
                    RetryReason.HANDLER_FAILED, Instant.now(), policyName, null, null);
        });
        return applied.get();
    }

    private void schedule(Connection conn, UUID eventId, String service, RetryKind kind,
                          int attempt, RetryReason reason, Instant nextAttemptAt,
                          String policyName, String errorType, String error) throws SQLException {
        RetryTask task = RetryTask.pending(
                        eventId, service, kind, attempt, reason, nextAttemptAt, policyName)
                .withNextAttempt(attempt, reason, nextAttemptAt, errorType, error);
        retryStore.schedule(conn, task);
    }

    private void deadOrSchedule(Connection conn, UUID eventId, String service, RetryKind kind,
                                RetryReason reason, String policyName,
                                String errorType, String error) throws SQLException {
        if (retryStore.find(conn, eventId, kind).isPresent()) {
            retryStore.dead(conn, eventId, kind, reason, errorType, error);
            return;
        }
        RetryTask dead = RetryTask.pending(
                        eventId, service, kind, 1, reason, Instant.now(), policyName)
                .withDead(reason, errorType, error);
        retryStore.schedule(conn, dead);
    }

    private RetryKind resolveKind(Connection conn, UUID eventId) throws SQLException {
        if (retryStore.find(conn, eventId, RetryKind.PROCESSING).isPresent()) {
            return RetryKind.PROCESSING;
        }
        if (retryStore.find(conn, eventId, RetryKind.DELIVERY).isPresent()) {
            return RetryKind.DELIVERY;
        }
        return RetryKind.PROCESSING;
    }

    private void inTransaction(TransactionWork work) {
        try (Connection conn = dataSource.getConnection()) {
            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                work.run(conn);
                conn.commit();
            } catch (SQLException | RuntimeException e) {
                rollback(conn, e);
                throw e;
            } finally {
                restoreAutoCommit(conn, autoCommit);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Lifecycle transaction failed", e);
        }
    }

    private void rollback(Connection conn, Exception cause) {
        try {
            conn.rollback();
        } catch (SQLException rollbackError) {
            cause.addSuppressed(rollbackError);
            log.warn("Failed to rollback lifecycle transaction", rollbackError);
        }
    }

    private void restoreAutoCommit(Connection conn, boolean autoCommit) {
        try {
            conn.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            log.warn("Failed to restore auto-commit state", e);
        }
    }

    @FunctionalInterface
    private interface TransactionWork {
        void run(Connection conn) throws SQLException;
    }
}
