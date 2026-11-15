package io.github.vovten.eventflow.transport.incoming.kafka;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-partition record tracking for the inbound Kafka transport.
 * <p>
 * Holds the processing futures of polled records, the in-flight counter, the
 * committed offset frontier and the failure block state of every partition.
 * <p>
 * <b>Thread-safety invariant:</b> the maps are mutated only by the consume-loop
 * thread. Handler threads only decrement the {@link AtomicInteger} instance
 * captured by their completion callback; they never touch the maps. Counters are
 * per-generation: when a partition is unblocked or revoked its entry is removed
 * and a later tracking starts a fresh counter — futures of the dropped
 * generation belong to intentionally discarded (re-delivered) records and do not
 * count towards backpressure of the new generation.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class PartitionStateTracker {

    private static final Logger log = LoggerFactory.getLogger(PartitionStateTracker.class);

    private final Map<TopicPartition, TreeMap<Long, CompletableFuture<?>>> pending = new HashMap<>();
    private final Map<TopicPartition, AtomicInteger> inFlight = new HashMap<>();
    private final Map<TopicPartition, Long> committedFrontiers = new HashMap<>();
    private final Map<TopicPartition, Long> blockedFrontiers = new HashMap<>();

    /**
     * Tracks a record processing future and increments the in-flight counter.
     *
     * @param partition  the record partition
     * @param offset     the record offset
     * @param processing the processing future
     */
    public void track(TopicPartition partition, long offset, CompletableFuture<?> processing) {
        pending.computeIfAbsent(partition, key -> new TreeMap<>()).put(offset, processing);
        AtomicInteger counter = inFlight.computeIfAbsent(partition, key -> new AtomicInteger());
        counter.incrementAndGet();
        processing.whenComplete((result, error) -> counter.decrementAndGet());
    }

    /**
     * Returns the partitions that currently have tracked records.
     *
     * @return unmodifiable set of partitions
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(pending.keySet());
    }

    /**
     * Advances the contiguous completed frontier of a partition and marks the
     * partition blocked when the head record failed.
     * <p>
     * <b>Side effects:</b> on the first failure records the blocked offset and
     * logs an error. The pending buffer is not mutated: completed records are
     * removed only after Kafka confirmed the commit (see
     * {@link #pruneCommitted}), so a failed commit can safely be retried with the
     * same frontier. Buffered records are intentionally kept so that records
     * which already completed are not reprocessed unnecessarily.
     *
     * @param partition the partition
     * @return the offset after the last contiguously completed record, or null
     *         when no new offset became committable
     */
    public Long advanceOrBlock(TopicPartition partition) {
        TreeMap<Long, CompletableFuture<?>> records = pending.get(partition);
        if (records == null) {
            return null;
        }
        Long frontier = null;
        for (Map.Entry<Long, CompletableFuture<?>> entry : records.entrySet()) {
            CompletableFuture<?> future = entry.getValue();
            if (!future.isDone()) {
                break;
            }
            if (future.isCompletedExceptionally()) {
                Long previous = blockedFrontiers.putIfAbsent(partition, entry.getKey());
                if (previous == null) {
                    log.error("Partition {} blocked at offset {}: processing failed; no further offsets "
                                    + "are committed or processed until rebalance, restart or unblock()",
                            partition, entry.getKey());
                }
                break;
            }
            frontier = entry.getKey() + 1;
        }
        return frontier;
    }

    /**
     * Computes the contiguous completed frontier without applying the failure
     * policy: the scan stops silently at the first pending or failed record.
     * <p>
     * Used during revocation and shutdown, when the partition is handed over or
     * the transport stops and a local failure must not be reported as a block.
     *
     * @param partition the partition
     * @return the offset after the last contiguously completed record, or null
     */
    public Long completedFrontierOnly(TopicPartition partition) {
        TreeMap<Long, CompletableFuture<?>> records = pending.get(partition);
        if (records == null) {
            return null;
        }
        Long frontier = null;
        for (Map.Entry<Long, CompletableFuture<?>> entry : records.entrySet()) {
            CompletableFuture<?> future = entry.getValue();
            if (!future.isDone() || future.isCompletedExceptionally()) {
                break;
            }
            frontier = entry.getKey() + 1;
        }
        return frontier;
    }

    /**
     * Returns the highest offset (exclusive) successfully committed for a
     * partition.
     *
     * @param partition the partition
     * @return the committed frontier, or null when nothing was committed yet
     */
    public Long committedFrontier(TopicPartition partition) {
        return committedFrontiers.get(partition);
    }

    /**
     * Remembers a confirmed commit frontier.
     *
     * @param partition the partition
     * @param frontier  the offset after the last committed record
     */
    public void markCommitted(TopicPartition partition, long frontier) {
        committedFrontiers.put(partition, frontier);
    }

    /**
     * Removes buffered records below a confirmed commit frontier.
     *
     * @param partition the partition
     * @param frontier  the committed offset (exclusive)
     */
    public void pruneCommitted(TopicPartition partition, long frontier) {
        TreeMap<Long, CompletableFuture<?>> records = pending.get(partition);
        if (records != null) {
            records.headMap(frontier, false).clear();
        }
    }

    /**
     * Checks whether a partition is blocked by a processing failure.
     *
     * @param partition the partition
     * @return true when the partition is blocked
     */
    public boolean isBlocked(TopicPartition partition) {
        return blockedFrontiers.containsKey(partition);
    }

    /**
     * Returns the first failed offset of a blocked partition.
     *
     * @param partition the partition
     * @return the blocked offset, or null when the partition is not blocked
     */
    public Long blockedAt(TopicPartition partition) {
        return blockedFrontiers.get(partition);
    }

    /**
     * Returns the number of records whose processing has not completed yet.
     *
     * @param partition the partition
     * @return number of not-yet-completed futures
     */
    public int inFlight(TopicPartition partition) {
        AtomicInteger counter = inFlight.get(partition);
        return counter == null ? 0 : counter.get();
    }

    /**
     * Returns the number of buffered (tracked) records of a partition.
     *
     * @param partition the partition
     * @return buffered record count
     */
    public int buffered(TopicPartition partition) {
        TreeMap<Long, CompletableFuture<?>> records = pending.get(partition);
        return records == null ? 0 : records.size();
    }

    /**
     * Drops the in-memory state of a single partition (used after unblock).
     *
     * @param partition the partition to reset
     */
    public void reset(TopicPartition partition) {
        pending.remove(partition);
        inFlight.remove(partition);
        blockedFrontiers.remove(partition);
    }

    /**
     * Drops all in-memory tracking state of the given partitions.
     *
     * @param partitions partitions to forget
     */
    public void clear(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            pending.remove(partition);
            inFlight.remove(partition);
            committedFrontiers.remove(partition);
            blockedFrontiers.remove(partition);
        }
    }
}
