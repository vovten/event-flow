package io.github.vovten.eventflow.transport.incoming;

import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.AsyncEventConsumer;
import io.github.vovten.eventflow.transport.InTransport;
import io.github.vovten.eventflow.transport.incoming.kafka.BackpressureController;
import io.github.vovten.eventflow.transport.incoming.kafka.KafkaConsumerProperties;
import io.github.vovten.eventflow.transport.incoming.kafka.KafkaOffsetCommitter;
import io.github.vovten.eventflow.transport.incoming.kafka.KafkaRebalanceListener;
import io.github.vovten.eventflow.transport.incoming.kafka.KafkaRecordDelivery;
import io.github.vovten.eventflow.transport.incoming.kafka.PartitionStateTracker;
import io.github.vovten.eventflow.transport.incoming.kafka.PartitionUnblockQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka incoming transport for receiving external events with
 * at-least-once delivery semantics.
 * <p>
 * This transport implements {@link AutoCloseable} to ensure proper resource cleanup.
 * Always close the transport when it's no longer needed:
 * <pre>{@code
 * try (KafkaInTransport transport = new KafkaInTransport("localhost:9092", "events", "group")) {
 *     transport.start(dispatcher::dispatch);
 *     // ... work ...
 * } // automatically stops and closes resources
 * }</pre>
 * <p>
 * This transport listens to one or more Kafka topics and delivers events
 * to the registered consumer. Event offsets are committed <b>only after</b> the
 * consumer's returned future has completed: a record's offset is never committed
 * before its processing finishes, so a crash re-delivers unfinished records
 * instead of losing them. Offsets are committed in contiguous order per
 * partition — a failed record blocks the commit of all later records of the
 * same partition.
 * <p>
 * <b>Failure policy.</b> When the processing of a record fails (an exceptional
 * future or a synchronous consumer exception), the partition is marked
 * <i>blocked</i> at that offset. From the next loop iteration the partition is
 * paused, the failed record is not committed, and no further records are handed
 * to the consumer. Records from the same poll batch that were already dispatched
 * keep running, but their results are not committed. This preserves
 * per-partition ordering. The buffered records are kept: already completed
 * records must not be reprocessed unnecessarily. Memory stays bounded because a
 * blocked partition is paused and the buffer has a hard cap.
 * <p>
 * A blocked partition is released by a rebalance/restart or explicitly via
 * {@link #unblock(TopicPartition)}: the operator fixes the root cause, calls
 * {@code unblock}, and the partition resumes from the failed offset. Note that
 * {@code unblock} intentionally re-delivers the failed record and everything
 * after it: handlers of the previous generation may still be running, so the
 * same event may be processed concurrently. Handlers must therefore be
 * idempotent, exactly as with any at-least-once delivery. A processing timeout
 * is the caller's responsibility — an event handler that never completes keeps
 * its partition paused once the buffer cap is reached.
 * <p>
 * <b>Backpressure.</b> A partition is paused when the number of in-flight
 * (not yet completed) records reaches
 * {@value #DEFAULT_MAX_IN_FLIGHT_RECORDS_PER_PARTITION}, or when its buffered
 * record count reaches the hard memory cap (twice the in-flight limit, which
 * protects against an indefinitely pending head record). The in-flight counter
 * deliberately ignores records that already completed behind a stuck head; the
 * buffer cap is what bounds memory and pauses such a partition. The partition
 * is resumed after the in-flight count and buffer size drop below the limits.
 * Because polling happens before the limits are evaluated, a single poll batch
 * (up to {@code max.poll.records}) may overshoot the thresholds; the buffer cap
 * is the actual upper bound, not the pause threshold.
 * <p>
 * <b>Rebalance.</b> On partition revocation the contiguous completed frontier is
 * committed before the in-memory tracking state is dropped. Lost partitions are
 * cleaned without a commit (ownership is already gone). The transport relies on
 * the standard Kafka rebalance callbacks, so cooperative assignors are
 * supported: partitions that stay assigned are not affected.
 * <p>
 * Deserialization failures (poison pills) are skipped permanently: such a
 * record can never be processed, so its offset is committed to avoid an endless
 * redelivery loop, and the failure is logged at ERROR level with the record
 * coordinates. A dead-letter policy is a planned follow-up.
 * <p>
 * Automatically detects event serialization format:
 * <ul>
 *   <li>Old JSON format (starts with '{' = 0x7B)</li>
 *   <li>New JSON format (magic byte 0x01)</li>
 *   <li>MessagePack format (magic byte 0x02)</li>
 * </ul>
 * <p>
 * <b>Default consumer settings (at-least-once):</b> {@code enable.auto.commit=false},
 * {@code auto.offset.reset=earliest} — a new consumer group starts from the
 * oldest retained record instead of silently skipping the backlog.
 * Custom {@link Properties} constructors may override these values.
 * <p>
 * A transport instance is <b>single-use</b>: after {@link #stop()} (or
 * {@link #close()}) it cannot be started again; {@link #start(AsyncEventConsumer)}
 * throws {@link IllegalStateException}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see EventSerializerFactory
 */
public class KafkaInTransport implements InTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaInTransport.class);

    private static final String NAME = "kafka";

    /**
     * Poll timeout in milliseconds.
     */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    /**
     * Maximum time to wait for the consume loop to finish on shutdown.
     */
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Maximum time the consumer may spend closing during shutdown.
     */
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Default number of in-flight records per partition before pausing.
     */
    static final int DEFAULT_MAX_IN_FLIGHT_RECORDS_PER_PARTITION = 5000;

    private final Consumer<String, byte[]> kafkaConsumer;
    private final List<String> topics;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final KafkaRecordDelivery recordDelivery;
    private final PartitionStateTracker tracker = new PartitionStateTracker();
    private final KafkaOffsetCommitter offsetCommitter = new KafkaOffsetCommitter();
    private final BackpressureController backpressure;
    private final PartitionUnblockQueue unblockQueue = new PartitionUnblockQueue();

    /**
     * Create Kafka transport with bootstrap servers, topics, and group ID.
     * Uses default EventSerializerFactory with JSON and MessagePack serializers.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topics           comma-separated list of topics to subscribe to
     * @param groupId          consumer group ID
     */
    public KafkaInTransport(String bootstrapServers, String topics, String groupId) {
        this(bootstrapServers, topics, groupId, new EventSerializerFactory());
    }

    /**
     * Create Kafka transport with bootstrap servers, topics, group ID, and custom serializer factory.
     *
     * @param bootstrapServers  Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topics            comma-separated list of topics to subscribe to
     * @param groupId           consumer group ID
     * @param serializerFactory serializer factory for event deserialization
     */
    public KafkaInTransport(String bootstrapServers, String topics, String groupId,
                            EventSerializerFactory serializerFactory) {
        this(createConsumer(bootstrapServers, groupId), parseTopics(topics),
                Executors.newSingleThreadExecutor(), serializerFactory);
    }

    /**
     * Create Kafka transport with custom properties, topics, and group ID.
     * Uses default EventSerializerFactory with JSON and MessagePack serializers.
     *
     * @param properties   Kafka consumer properties
     * @param topics       comma-separated list of topics to subscribe to
     * @param groupId      consumer group ID
     */
    public KafkaInTransport(Properties properties, String topics, String groupId) {
        this(properties, topics, groupId, new EventSerializerFactory());
    }

    /**
     * Create Kafka transport with custom properties, topics, group ID, and serializer factory.
     *
     * @param properties        Kafka consumer properties
     * @param topics            comma-separated list of topics to subscribe to
     * @param groupId           consumer group ID
     * @param serializerFactory serializer factory for event deserialization
     */
    public KafkaInTransport(Properties properties, String topics, String groupId,
                            EventSerializerFactory serializerFactory) {
        this(createConsumer(properties, groupId), parseTopics(topics),
                Executors.newSingleThreadExecutor(), serializerFactory);
    }

    /**
     * Create Kafka transport with custom consumer (for tests).
     * Uses default EventSerializerFactory with JSON and MessagePack serializers.
     *
     * @param kafkaConsumer Kafka consumer instance
     * @param topics        list of topics to subscribe to
     */
    public KafkaInTransport(Consumer<String, byte[]> kafkaConsumer, List<String> topics) {
        this(kafkaConsumer, topics, Executors.newSingleThreadExecutor(), new EventSerializerFactory());
    }

    /**
     * Create Kafka transport with custom consumer and serializer factory.
     *
     * @param kafkaConsumer      Kafka consumer instance
     * @param topics             list of topics to subscribe to
     * @param serializerFactory  serializer factory for event deserialization
     */
    public KafkaInTransport(Consumer<String, byte[]> kafkaConsumer,
                            List<String> topics,
                            EventSerializerFactory serializerFactory) {
        this(kafkaConsumer, topics, Executors.newSingleThreadExecutor(), serializerFactory);
    }

    /**
     * Create Kafka transport with custom consumer, executor, and serializer factory.
     *
     * @param kafkaConsumer      Kafka consumer instance
     * @param topics             list of topics to subscribe to
     * @param executorService    executor service for running the consumer loop
     * @param serializerFactory  serializer factory for event deserialization
     */
    public KafkaInTransport(Consumer<String, byte[]> kafkaConsumer,
                            List<String> topics,
                            ExecutorService executorService,
                            EventSerializerFactory serializerFactory) {
        this(kafkaConsumer, topics, executorService, serializerFactory,
                DEFAULT_MAX_IN_FLIGHT_RECORDS_PER_PARTITION);
    }

    /**
     * Create Kafka transport with an explicit backpressure limit (for tests).
     *
     * @param kafkaConsumer                  Kafka consumer instance
     * @param topics                         list of topics to subscribe to
     * @param executorService                executor service for the consumer loop
     * @param serializerFactory              serializer factory for event deserialization
     * @param maxInFlightRecordsPerPartition per-partition in-flight limit
     */
    KafkaInTransport(Consumer<String, byte[]> kafkaConsumer,
                     List<String> topics,
                     ExecutorService executorService,
                     EventSerializerFactory serializerFactory,
                     int maxInFlightRecordsPerPartition) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("At least one Kafka topic must be configured");
        }
        this.topics = List.copyOf(topics);
        this.kafkaConsumer = Objects.requireNonNull(kafkaConsumer, "kafkaConsumer must not be null");
        this.executorService = Objects.requireNonNull(executorService, "executorService must not be null");
        this.recordDelivery = new KafkaRecordDelivery(
                Objects.requireNonNull(serializerFactory, "serializerFactory must not be null"), NAME);
        this.backpressure = new BackpressureController(maxInFlightRecordsPerPartition);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void start(AsyncEventConsumer eventConsumer) {
        if (closed.get()) {
            throw new IllegalStateException("KafkaInTransport has been stopped and cannot be restarted");
        }
        if (!running.compareAndSet(false, true)) {
            log.warn("KafkaInTransport is already running");
            return;
        }
        try {
            executorService.execute(() -> consumeLoop(eventConsumer));
            log.info("KafkaInTransport started, listening to topics: {}", topics);
        } catch (RuntimeException e) {
            // The executor may already be shut down; do not leave the transport
            // in a phantom "running" state.
            running.set(false);
            closed.set(true);
            throw new IllegalStateException("KafkaInTransport could not start its consume loop", e);
        }
    }

    @Override
    public void stop() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Stopping KafkaInTransport...");
        boolean wasRunning = running.compareAndSet(true, false);
        if (wasRunning) {
            // Unblock poll()/commitSync() on the consume-loop thread; the
            // consumer is closed by the loop thread itself in its finally block.
            kafkaConsumer.wakeup();
        }
        unblockQueue.clear();
        // Shut the executor down regardless of how the loop ended: a fatal
        // consumer error sets running=false and would otherwise leak the thread.
        executorService.shutdown();
        awaitTermination();
        log.info("KafkaInTransport stopped");
    }

    /**
     * Requests the release of a blocked partition.
     * <p>
     * Use it after the root cause of the processing failure has been fixed: the
     * partition is re-positioned at the failed offset, resumed, and reprocessed.
     * The request is executed asynchronously by the consume-loop thread, so this
     * method is safe to call from any thread. The call is fire-and-forget: the
     * request is silently ignored (debug log) when the partition is not blocked,
     * no longer assigned, or the transport has been stopped — a rebalance may
     * have released it in the meantime.
     * <p>
     * <b>Duplicates are expected:</b> the failed record and every record after it
     * are re-delivered. Handlers of the previous generation may still be running,
     * so the same event can be processed concurrently. Handlers must be
     * idempotent; completion of the old generation does not affect the new one.
     *
     * @param partition the blocked partition to release (must not be null)
     */
    public void unblock(TopicPartition partition) {
        Objects.requireNonNull(partition, "partition must not be null");
        if (!running.get()) {
            log.debug("Ignoring unblock request for {}: transport is not running", partition);
            return;
        }
        unblockQueue.request(partition);
    }

    /**
     * Close the transport and release all resources (consumer and executor).
     * <p>
     * This method is idempotent and safe to call multiple times. It waits up to
     * {@link #SHUTDOWN_TIMEOUT} for the consume loop to perform a best-effort
     * final offset commit and close the consumer. If the loop cannot finish
     * (for example, the broker is unreachable), it is interrupted and woken up;
     * the final commit is then abandoned. Note that an in-flight
     * {@code Consumer.commitSync} may still block up to the Kafka
     * {@code default.api.timeout.ms}; the consumer close itself is bounded by
     * {@link #CLOSE_TIMEOUT}.
     */
    @Override
    public void close() {
        stop();
    }

    private void awaitTermination() {
        try {
            if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                log.warn("Consumer loop did not terminate within {}; forcing shutdown "
                        + "(final commit is best-effort)", SHUTDOWN_TIMEOUT);
                executorService.shutdownNow();
                // Interrupt a commitSync that started after the first wakeup:
                // interrupt alone does not abort a Kafka commit, wakeup does.
                kafkaConsumer.wakeup();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for the consumer loop to stop");
        }
    }

    private void consumeLoop(AsyncEventConsumer eventConsumer) {
        try {
            kafkaConsumer.subscribe(topics,
                    new KafkaRebalanceListener(kafkaConsumer, tracker, offsetCommitter, backpressure));
            while (running.get()) {
                unblockQueue.process(kafkaConsumer, tracker, backpressure);
                pollAndDeliver(kafkaConsumer, eventConsumer);
                if (!running.get()) {
                    break;
                }
                commitCompletedOffsets(kafkaConsumer);
                backpressure.apply(kafkaConsumer, tracker);
                backpressure.release(kafkaConsumer, tracker);
            }
        } catch (WakeupException e) {
            log.debug("KafkaInTransport loop woken up for shutdown");
        } catch (Exception e) {
            if (running.get()) {
                log.error("KafkaInTransport loop stopped due to a consumer error", e);
                running.set(false);
            } else {
                log.debug("KafkaInTransport loop terminated during shutdown ({}): {}",
                        e.getClass().getSimpleName(), e.getMessage());
            }
        } finally {
            try {
                commitFinalOffsets(kafkaConsumer);
            } catch (WakeupException e) {
                log.debug("Final offset commit skipped: transport is stopping");
            } catch (Exception e) {
                log.warn("Final offset commit failed: {}: {}", e.getClass().getSimpleName(), e.getMessage());
            }
            tryCloseConsumer();
            // Never leak the consume-loop thread when the loop ended on its own
            // (fatal consumer error) rather than through stop().
            executorService.shutdown();
        }
    }

    /**
     * Polls records and submits each one to the consumer.
     * <p>
     * A transient (retriable) error is logged and retried on the next poll. A
     * non-retriable error is rethrown so the consume loop stops immediately
     * instead of continuing the current iteration with a broken consumer.
     *
     * @param consumer      the Kafka consumer
     * @param eventConsumer the event consumer
     */
    private void pollAndDeliver(Consumer<String, byte[]> consumer, AsyncEventConsumer eventConsumer) {
        try {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(POLL_TIMEOUT)) {
                CompletableFuture<?> processing = recordDelivery.deliver(record, eventConsumer);
                tracker.track(new TopicPartition(record.topic(), record.partition()),
                        record.offset(), processing);
            }
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            if (running.get() && e instanceof RetriableException) {
                log.warn("Retriable Kafka consumer error; will retry on next poll: {}: {}",
                        e.getClass().getSimpleName(), e.getMessage());
                return;
            }
            throw e;
        }
    }

    private void commitCompletedOffsets(Consumer<String, byte[]> consumer) {
        commitFrontiers(consumer, committableFrontiers(), "will retry on the next loop iteration");
    }

    /**
     * Final best-effort commit used when the loop is terminating. Unlike the
     * regular path it never applies the failure policy: a shutdown must not mark
     * a partition blocked or emit a misleading ERROR for a pending/failed head.
     *
     * @param consumer the Kafka consumer
     */
    private void commitFinalOffsets(Consumer<String, byte[]> consumer) {
        commitFrontiers(consumer, finalFrontiers(), "final commit is best-effort");
    }

    private Map<TopicPartition, Long> committableFrontiers() {
        Map<TopicPartition, Long> frontiers = new HashMap<>();
        for (TopicPartition partition : tracker.partitions()) {
            addIfAdvanced(frontiers, partition, tracker.advanceOrBlock(partition));
        }
        return frontiers;
    }

    private Map<TopicPartition, Long> finalFrontiers() {
        Map<TopicPartition, Long> frontiers = new HashMap<>();
        for (TopicPartition partition : tracker.partitions()) {
            addIfAdvanced(frontiers, partition, tracker.completedFrontierOnly(partition));
        }
        return frontiers;
    }

    private void addIfAdvanced(Map<TopicPartition, Long> frontiers, TopicPartition partition, Long frontier) {
        Long committed = tracker.committedFrontier(partition);
        if (frontier != null && (committed == null || frontier > committed)) {
            frontiers.put(partition, frontier);
        }
    }

    private void commitFrontiers(Consumer<String, byte[]> consumer,
                                 Map<TopicPartition, Long> frontiers, String retryHint) {
        Map<TopicPartition, Long> confirmed = offsetCommitter.commit(consumer, frontiers, retryHint);
        // Completed records are removed only after Kafka confirmed the commit:
        // if commitSync fails, the frontier stays recoverable and is retried.
        confirmed.forEach((partition, frontier) -> {
            tracker.markCommitted(partition, frontier);
            tracker.pruneCommitted(partition, frontier);
        });
    }

    private void tryCloseConsumer() {
        try {
            kafkaConsumer.close(CLOSE_TIMEOUT);
        } catch (Exception e) {
            log.warn("Error closing consumer", e);
        }
    }

    /**
     * Builds the default consumer properties for at-least-once delivery.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId          consumer group ID
     * @return consumer properties
     */
    static Properties defaultConsumerProperties(String bootstrapServers, String groupId) {
        return KafkaConsumerProperties.defaults(bootstrapServers, groupId);
    }

    /**
     * Splits a comma-separated topic list, trimming each name and ignoring blank
     * entries.
     *
     * @param topics comma-separated topics
     * @return list of topic names
     * @throws IllegalArgumentException if {@code topics} is null
     */
    static List<String> parseTopics(String topics) {
        return KafkaConsumerProperties.parseTopics(topics);
    }

    private static Consumer<String, byte[]> createConsumer(String bootstrapServers, String groupId) {
        return new KafkaConsumer<>(KafkaConsumerProperties.defaults(bootstrapServers, groupId));
    }

    private static Consumer<String, byte[]> createConsumer(Properties properties, String groupId) {
        return new KafkaConsumer<>(KafkaConsumerProperties.withOverrides(properties, groupId));
    }
}
