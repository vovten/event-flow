package io.github.vovten.eventflow.transport.incoming.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Rebalance listener for the inbound Kafka transport.
 * <p>
 * On revocation it commits the contiguous completed frontier of the revoked
 * partitions (so the next owner continues from the acknowledged position) and
 * then drops their in-memory state. Assignment and loss only drop the state:
 * ownership is already gone for lost partitions, committing would fail there.
 * <p>
 * A failure detected during revocation is not reported as a block: the partition
 * is being handed over, so uncommitted records are simply re-delivered by the new
 * owner.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class KafkaRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaRebalanceListener.class);

    private final Consumer<String, byte[]> consumer;
    private final PartitionStateTracker tracker;
    private final KafkaOffsetCommitter committer;
    private final BackpressureController backpressure;

    /**
     * Creates the rebalance listener.
     *
     * @param consumer     the Kafka consumer
     * @param tracker      the partition state tracker
     * @param committer    the offset committer
     * @param backpressure the backpressure controller
     */
    public KafkaRebalanceListener(Consumer<String, byte[]> consumer,
                                  PartitionStateTracker tracker,
                                  KafkaOffsetCommitter committer,
                                  BackpressureController backpressure) {
        this.consumer = consumer;
        this.tracker = tracker;
        this.committer = committer;
        this.backpressure = backpressure;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        commitRevokedPartitions(partitions);
        clearPartitionState(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Defensive: a fresh assignment must not inherit stale state.
        clearPartitionState(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        // Ownership is already gone, committing would fail.
        clearPartitionState(partitions);
    }

    private void commitRevokedPartitions(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> frontiers = new HashMap<>();
        for (TopicPartition partition : partitions) {
            // Ownership is moving: a failure is the new owner's concern, so do
            // not mark the partition blocked or log an error here.
            Long frontier = tracker.completedFrontierOnly(partition);
            Long committed = tracker.committedFrontier(partition);
            if (frontier != null && (committed == null || frontier > committed)) {
                frontiers.put(partition, frontier);
            }
        }
        committer.commit(consumer, frontiers,
                        "commit during revocation failed; the new owner will re-deliver uncommitted records")
                .forEach(tracker::markCommitted);
    }

    private void clearPartitionState(Collection<TopicPartition> partitions) {
        tracker.clear(partitions);
        backpressure.clear(partitions);
        if (log.isDebugEnabled()) {
            log.debug("Dropped in-memory state for partitions: {}", partitions);
        }
    }
}
