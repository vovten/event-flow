package io.github.vovten.eventflow.transport.incoming.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread-safe queue of partition unblock requests processed by the consume loop.
 * <p>
 * {@link #request(TopicPartition)} may be called from any thread. The
 * consumer-side changes ({@code seek} + {@code resume}) are applied first and the
 * in-memory state is dropped only on success: if {@code seek}/{@code resume}
 * fails, the partition stays blocked and the request can be retried.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class PartitionUnblockQueue {

    private static final Logger log = LoggerFactory.getLogger(PartitionUnblockQueue.class);

    private final Queue<TopicPartition> requests = new ConcurrentLinkedQueue<>();

    /**
     * Enqueues an unblock request.
     *
     * @param partition the partition to unblock (must not be null)
     */
    public void request(TopicPartition partition) {
        requests.add(Objects.requireNonNull(partition, "partition must not be null"));
    }

    /**
     * Processes all pending unblock requests. Must run on the consume-loop
     * thread, which owns the Kafka consumer.
     *
     * @param consumer     the Kafka consumer
     * @param tracker      the partition state tracker
     * @param backpressure the backpressure controller
     */
    public void process(Consumer<String, byte[]> consumer, PartitionStateTracker tracker,
                        BackpressureController backpressure) {
        TopicPartition partition;
        while ((partition = requests.poll()) != null) {
            Long blockedAt = tracker.blockedAt(partition);
            if (blockedAt == null) {
                log.debug("Ignoring unblock request for non-blocked partition {}", partition);
                continue;
            }
            try {
                consumer.seek(partition, blockedAt);
                consumer.resume(List.of(partition));
            } catch (RuntimeException e) {
                log.warn("Failed to unblock partition {} ({}: {}); it stays blocked and will be retried",
                        partition, e.getClass().getSimpleName(), e.getMessage());
                continue;
            }
            tracker.reset(partition);
            backpressure.clear(List.of(partition));
            log.info("Unblocked partition {}; reprocessing from offset {}", partition, blockedAt);
        }
    }

    /**
     * Drops all pending requests (used on shutdown).
     */
    public void clear() {
        requests.clear();
    }
}
