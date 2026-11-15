package io.github.vovten.eventflow.transport.incoming.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Commits offset frontiers to Kafka and reports which ones were confirmed.
 * <p>
 * A confirm-based contract lets the caller update its bookkeeping and prune the
 * pending buffer only after Kafka actually accepted the commit: a transient
 * failure keeps the same frontier recoverable and it is retried on the next loop
 * iteration.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class KafkaOffsetCommitter {

    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetCommitter.class);

    /**
     * Commits the given frontiers.
     *
     * @param consumer  the Kafka consumer
     * @param frontiers partition to offset (exclusive) to commit
     * @param retryHint explanation of what happens after a failed commit
     * @return the frontiers Kafka confirmed; empty when nothing was committed
     * @throws WakeupException if the consumer is woken up while committing
     */
    public Map<TopicPartition, Long> commit(Consumer<String, byte[]> consumer,
                                            Map<TopicPartition, Long> frontiers, String retryHint) {
        if (frontiers.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        frontiers.forEach((partition, offset) -> toCommit.put(partition, new OffsetAndMetadata(offset)));
        try {
            consumer.commitSync(toCommit);
            if (log.isDebugEnabled()) {
                log.debug("Committed offsets: {}", toCommit);
            }
            return frontiers;
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Failed to commit offsets {} ({}): {}: {}",
                    toCommit.keySet(), retryHint, e.getClass().getSimpleName(), e.getMessage());
            return Map.of();
        }
    }
}
