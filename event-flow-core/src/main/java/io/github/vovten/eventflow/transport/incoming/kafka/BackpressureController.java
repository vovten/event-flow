package io.github.vovten.eventflow.transport.incoming.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Pause/resume controller implementing per-partition backpressure.
 * <p>
 * A partition is paused when it is blocked by a failure, when its in-flight
 * record count reaches the configured limit, or when its buffer reaches the hard
 * memory cap (twice the in-flight limit). It is resumed once both counts drop
 * below the limits. Only partitions currently assigned to the consumer are
 * resumed; blocked partitions are never resumed automatically.
 * <p>
 * All methods are expected to be called from the consume-loop thread. Pause and
 * resume are local consumer operations: a failure is logged and retried on the
 * next loop iteration instead of failing the consume loop.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class BackpressureController {

    private static final Logger log = LoggerFactory.getLogger(BackpressureController.class);

    private final int maxInFlight;
    private final int maxBuffered;
    private final Set<TopicPartition> paused = new HashSet<>();

    /**
     * Creates the controller.
     *
     * @param maxInFlightRecordsPerPartition per-partition in-flight limit (must be positive)
     * @throws IllegalArgumentException if the limit is not positive
     */
    public BackpressureController(int maxInFlightRecordsPerPartition) {
        if (maxInFlightRecordsPerPartition <= 0) {
            throw new IllegalArgumentException("maxInFlightRecordsPerPartition must be positive");
        }
        this.maxInFlight = maxInFlightRecordsPerPartition;
        // Guard against int overflow for very large limits.
        long buffered = (long) maxInFlightRecordsPerPartition * 2;
        this.maxBuffered = (int) Math.min(buffered, Integer.MAX_VALUE);
    }

    /**
     * Pauses partitions that are blocked, have too many in-flight records, or
     * have reached the hard buffer cap.
     *
     * @param consumer the Kafka consumer
     * @param tracker  the partition state tracker
     */
    public void apply(Consumer<String, byte[]> consumer, PartitionStateTracker tracker) {
        List<TopicPartition> toPause = new ArrayList<>();
        for (TopicPartition partition : tracker.partitions()) {
            if (paused.contains(partition)) {
                continue;
            }
            boolean blocked = tracker.isBlocked(partition);
            boolean tooManyInFlight = tracker.inFlight(partition) >= maxInFlight;
            boolean bufferFull = tracker.buffered(partition) >= maxBuffered;
            if (blocked || tooManyInFlight || bufferFull) {
                toPause.add(partition);
            }
        }
        if (toPause.isEmpty()) {
            return;
        }
        try {
            consumer.pause(toPause);
            paused.addAll(toPause);
            log.debug("Paused partitions due to backpressure or failure: {}", toPause);
        } catch (RuntimeException e) {
            // pause() is a local operation; a failure must not kill the consume
            // loop. The pause is retried on the next iteration.
            log.warn("Failed to pause partitions {} ({}: {})",
                    toPause, e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * Resumes paused, assigned partitions whose in-flight count and buffer size
     * have dropped below the limits.
     *
     * @param consumer the Kafka consumer
     * @param tracker  the partition state tracker
     */
    public void release(Consumer<String, byte[]> consumer, PartitionStateTracker tracker) {
        Set<TopicPartition> assigned = assignedPartitions(consumer);
        List<TopicPartition> toResume = new ArrayList<>();
        for (TopicPartition partition : paused) {
            if (tracker.isBlocked(partition) || !assigned.contains(partition)) {
                continue;
            }
            if (tracker.inFlight(partition) < maxInFlight && tracker.buffered(partition) < maxBuffered) {
                toResume.add(partition);
            }
        }
        if (toResume.isEmpty()) {
            return;
        }
        try {
            consumer.resume(toResume);
            toResume.forEach(paused::remove);
            log.debug("Resumed partitions after backpressure: {}", toResume);
        } catch (RuntimeException e) {
            // resume() is a local operation; keep the paused bookkeeping and
            // retry on the next iteration instead of killing the loop.
            log.warn("Failed to resume partitions {} ({}: {})",
                    toResume, e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * Forgets the pause bookkeeping of the given partitions (rebalance/unblock).
     *
     * @param partitions partitions to forget
     */
    public void clear(Collection<TopicPartition> partitions) {
        partitions.forEach(paused::remove);
    }

    private static Set<TopicPartition> assignedPartitions(Consumer<String, byte[]> consumer) {
        try {
            return consumer.assignment();
        } catch (RuntimeException e) {
            log.debug("Failed to read consumer assignment ({}): {}",
                    e.getClass().getSimpleName(), e.getMessage());
            return Set.of();
        }
    }
}
