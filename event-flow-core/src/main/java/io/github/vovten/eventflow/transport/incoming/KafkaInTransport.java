package io.github.vovten.eventflow.transport.incoming;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializationException;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.AsyncEventConsumer;
import io.github.vovten.eventflow.transport.InTransport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Cross-application event communication</li>
 *   <li>Event-driven microservices architecture</li>
 *   <li>Receiving events from external systems</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see EventSerializerFactory
 */
public class KafkaInTransport implements InTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaInTransport.class);

    private static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";

    private static final String MDC_DELIVERED_FROM = "deliveredFrom";

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
    private final EventSerializerFactory serializerFactory;

    /**
     * Offsets of records polled in this session, ordered per partition.
     * A record stays in the map until the contiguous commit frontier passes
     * it; records behind a stuck head remain buffered (bounded by the hard
     * buffer cap). Only the consume-loop thread mutates this map, so no
     * synchronization is required. A record that never completes keeps its
     * partition paused once the buffer cap is reached.
     */
    private final Map<TopicPartition, TreeMap<Long, CompletableFuture<?>>> pendingByPartition = new HashMap<>();

    /**
     * Number of not-yet-completed futures per partition. Incremented when a
     * record is tracked and decremented by the completion callback, which may
     * run on a handler thread — hence the atomic counter value.
     * <p>
     * <b>Thread-safety invariant:</b> the {@code HashMap} itself is mutated only
     * by the consume-loop thread ({@code computeIfAbsent}/{@code get}/{@code remove});
     * handler threads only decrement the {@link AtomicInteger} instance captured
     * by their callback, never touching the map.
     * <p>
     * Counters are per-generation: when a partition is unblocked or revoked, its
     * entry is removed and a later tracking starts a fresh counter. Futures left
     * over from the dropped generation belong to intentionally discarded work
     * (the records are re-delivered) and therefore do not count towards
     * backpressure of the new generation.
     */
    private final Map<TopicPartition, AtomicInteger> inFlightByPartition = new HashMap<>();

    /**
     * Highest offset (exclusive) successfully committed per partition. Guards
     * against re-committing after a transient commit failure: the frontier is
     * retried on the next loop iteration.
     */
    private final Map<TopicPartition, Long> committedFrontiers = new HashMap<>();

    /**
     * First offset whose processing failed per partition. A blocked partition
     * cannot advance its frontier and stays paused until a rebalance, restart
     * or an explicit {@link #unblock(TopicPartition)}.
     */
    private final Map<TopicPartition, Long> blockedFrontiers = new HashMap<>();

    /**
     * Partitions currently paused for backpressure or because they are blocked.
     */
    private final Set<TopicPartition> pausedPartitions = new HashSet<>();

    /**
     * Unblock requests from other threads, processed by the consume loop.
     */
    private final Queue<TopicPartition> unblockRequests = new ConcurrentLinkedQueue<>();

    /**
     * Maximum number of in-flight records per partition before pausing.
     */
    private final int maxInFlightRecordsPerPartition;

    /**
     * Hard cap on buffered records per partition; protects memory when a head
     * record stays pending while later records complete.
     */
    private final int maxBufferedRecordsPerPartition;

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
        if (maxInFlightRecordsPerPartition <= 0) {
            throw new IllegalArgumentException("maxInFlightRecordsPerPartition must be positive");
        }
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("At least one Kafka topic must be configured");
        }
        this.topics = List.copyOf(topics);
        this.kafkaConsumer = Objects.requireNonNull(kafkaConsumer, "kafkaConsumer must not be null");
        this.executorService = Objects.requireNonNull(executorService, "executorService must not be null");
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory must not be null");
        this.maxInFlightRecordsPerPartition = maxInFlightRecordsPerPartition;
        // Guard against int overflow for very large limits.
        long buffered = (long) maxInFlightRecordsPerPartition * 2;
        this.maxBufferedRecordsPerPartition = (int) Math.min(buffered, Integer.MAX_VALUE);
    }

    @Override
    public String name() {
        return "kafka";
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
        unblockRequests.clear();
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
        unblockRequests.add(partition);
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
            kafkaConsumer.subscribe(topics, rebalanceListener(kafkaConsumer));
            while (running.get()) {
                processUnblockRequests(kafkaConsumer);
                pollAndDeliver(kafkaConsumer, eventConsumer);
                if (!running.get()) {
                    break;
                }
                commitCompletedOffsets(kafkaConsumer);
                applyBackpressure(kafkaConsumer);
                releaseBackpressure(kafkaConsumer);
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
                CompletableFuture<?> processing = tryDeliverEvent(record, eventConsumer);
                trackRecord(new TopicPartition(record.topic(), record.partition()),
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

    /**
     * Stores the processing future of a record and keeps the in-flight counter
     * up to date.
     *
     * @param partition  the record partition
     * @param offset     the record offset
     * @param processing the processing future
     */
    private void trackRecord(TopicPartition partition, long offset, CompletableFuture<?> processing) {
        pendingByPartition.computeIfAbsent(partition, key -> new TreeMap<>()).put(offset, processing);
        AtomicInteger inFlight = inFlightByPartition.computeIfAbsent(partition, key -> new AtomicInteger());
        inFlight.incrementAndGet();
        processing.whenComplete((result, error) -> inFlight.decrementAndGet());
    }

    /**
     * Releases partitions requested via {@link #unblock(TopicPartition)}:
     * re-positions them at the failed offset, resumes them and drops their
     * buffered state. Executed on the consume-loop thread.
     *
     * @param consumer the Kafka consumer
     */
    private void processUnblockRequests(Consumer<String, byte[]> consumer) {
        TopicPartition partition;
        while ((partition = unblockRequests.poll()) != null) {
            Long blockedAt = blockedFrontiers.get(partition);
            if (blockedAt == null) {
                log.debug("Ignoring unblock request for non-blocked partition {}", partition);
                continue;
            }
            // Apply the consumer-side changes first: if seek/resume fails the
            // bookkeeping must stay intact so the request can be retried and the
            // partition is not left paused without anyone tracking it.
            try {
                consumer.seek(partition, blockedAt);
                consumer.resume(List.of(partition));
            } catch (RuntimeException e) {
                log.warn("Failed to unblock partition {} ({}: {}); it stays blocked and will be retried",
                        partition, e.getClass().getSimpleName(), e.getMessage());
                continue;
            }
            blockedFrontiers.remove(partition);
            pendingByPartition.remove(partition);
            inFlightByPartition.remove(partition);
            pausedPartitions.remove(partition);
            log.info("Unblocked partition {}; reprocessing from offset {}", partition, blockedAt);
        }
    }

    /**
     * Commits the offset after the last contiguously completed record of each
     * partition. A record whose processing failed (exceptional future) or has
     * not completed yet stops the frontier: nothing after it is committed, so
     * those records are re-delivered after a restart.
     *
     * @param consumer the Kafka consumer
     */
    private void commitCompletedOffsets(Consumer<String, byte[]> consumer) {
        commitCompletedOffsets(consumer, "will retry on the next loop iteration");
    }

    /**
     * Commits the completed offsets, using the hint in the failure log.
     *
     * @param consumer  the Kafka consumer
     * @param retryHint explanation of what happens after a failed commit
     */
    private void commitCompletedOffsets(Consumer<String, byte[]> consumer, String retryHint) {
        Map<TopicPartition, Long> frontiers = new HashMap<>();
        for (Map.Entry<TopicPartition, TreeMap<Long, CompletableFuture<?>>> entry : pendingByPartition.entrySet()) {
            TopicPartition partition = entry.getKey();
            Long frontier = advanceOrBlock(partition, entry.getValue());
            Long committed = committedFrontiers.get(partition);
            if (frontier != null && (committed == null || frontier > committed)) {
                frontiers.put(partition, frontier);
            }
        }
        Map<TopicPartition, Long> committedNow = commitFrontiers(consumer, frontiers, retryHint);
        // Completed records are removed only after Kafka confirmed the commit:
        // if commitSync fails, the frontier stays recoverable and is retried.
        committedNow.forEach(this::pruneCommitted);
    }

    /**
     * Final best-effort commit used when the loop is terminating. Unlike the
     * regular path it never applies the failure policy: a shutdown must not mark
     * a partition blocked or emit a misleading ERROR for a pending/failed head.
     *
     * @param consumer the Kafka consumer
     */
    private void commitFinalOffsets(Consumer<String, byte[]> consumer) {
        Map<TopicPartition, Long> frontiers = new HashMap<>();
        for (Map.Entry<TopicPartition, TreeMap<Long, CompletableFuture<?>>> entry : pendingByPartition.entrySet()) {
            TopicPartition partition = entry.getKey();
            Long frontier = completedFrontierOnly(entry.getValue());
            Long committed = committedFrontiers.get(partition);
            if (frontier != null && (committed == null || frontier > committed)) {
                frontiers.put(partition, frontier);
            }
        }
        commitFrontiers(consumer, frontiers, "final commit is best-effort");
    }

    /**
     * Removes buffered records below a confirmed commit frontier.
     *
     * @param partition the partition
     * @param frontier  the committed offset (exclusive)
     */
    private void pruneCommitted(TopicPartition partition, long frontier) {
        TreeMap<Long, CompletableFuture<?>> pending = pendingByPartition.get(partition);
        if (pending != null) {
            pending.headMap(frontier, false).clear();
        }
    }

    /**
     * Computes the contiguous completed frontier without applying the failure
     * policy: the scan stops silently at the first pending or failed record.
     * <p>
     * Used during revocation, when the partition is handed to another consumer
     * and a local failure must not be reported as a block.
     *
     * @param pending offsets of in-session records ordered by offset
     * @return the offset after the last contiguously completed record, or null
     */
    private Long completedFrontierOnly(TreeMap<Long, CompletableFuture<?>> pending) {
        Long frontier = null;
        for (Map.Entry<Long, CompletableFuture<?>> entry : pending.entrySet()) {
            CompletableFuture<?> future = entry.getValue();
            if (!future.isDone() || future.isCompletedExceptionally()) {
                break;
            }
            frontier = entry.getKey() + 1;
        }
        return frontier;
    }

    /**
     * Advances the contiguous completed frontier of a partition and marks the
     * partition blocked when the head record failed.
     * <p>
     * <b>Side effects:</b> on the first failure records the blocked offset and
     * logs an error. {@code pending} is not mutated: completed records are
     * removed only after Kafka confirmed the commit (see {@link #pruneCommitted}),
     * so a failed commit can safely be retried with the same frontier. Buffered
     * records are intentionally kept: records that already completed must not be
     * reprocessed after a restart of the same session state. Memory stays bounded
     * because a blocked partition is paused.
     *
     * @param partition the partition the pending map belongs to
     * @param pending   offsets of in-session records ordered by offset
     * @return the offset after the last contiguously completed record, or null
     *         when no new offset became committable
     */
    private Long advanceOrBlock(TopicPartition partition, TreeMap<Long, CompletableFuture<?>> pending) {
        Long frontier = null;
        for (Map.Entry<Long, CompletableFuture<?>> entry : pending.entrySet()) {
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
     * Commits the given frontiers and remembers them. A transient commit
     * failure leaves the pending state untouched so the same frontier is
     * retried on the next loop iteration.
     *
     * @param consumer  the Kafka consumer
     * @param frontiers partition to offset (exclusive) to commit
     * @param retryHint explanation of what happens after a failed commit
     * @return the frontiers Kafka confirmed; empty when the commit failed
     */
    private Map<TopicPartition, Long> commitFrontiers(Consumer<String, byte[]> consumer,
                                                      Map<TopicPartition, Long> frontiers, String retryHint) {
        if (frontiers.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        frontiers.forEach((partition, offset) -> toCommit.put(partition, new OffsetAndMetadata(offset)));
        try {
            consumer.commitSync(toCommit);
            toCommit.forEach((partition, offset) -> committedFrontiers.put(partition, offset.offset()));
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

    /**
     * Deserializes and delivers a single record.
     *
     * @param record        the Kafka record
     * @param eventConsumer the event consumer
     * @return the processing future to track for offset commitment; poison
     *         pills return an already completed future so that the offset
     *         frontier advances past them (skip)
     */
    private CompletableFuture<?> tryDeliverEvent(ConsumerRecord<String, byte[]> record,
                                                 AsyncEventConsumer eventConsumer) {
        byte[] data = record.value();
        if (data == null) {
            // Kafka tombstone: not an event. Skip and advance the frontier,
            // otherwise it would be re-delivered forever.
            log.warn("Skipping tombstone record from topic: {}, partition: {}, offset: {} — "
                            + "its offset will be committed",
                    record.topic(), record.partition(), record.offset());
            return CompletableFuture.completedFuture(null);
        }
        try {
            EventSerializer serializer = serializerFactory.getByData(data);
            Event event = serializer.deserialize(data, Event.class);
            if (event == null) {
                throw new EventSerializationException("Deserializer returned null for event data");
            }
            return deliverEvent(record, event, eventConsumer);
        } catch (EventSerializationException e) {
            // Poison pill: the record can never be deserialized, skipping its
            // offset avoids an endless redelivery loop. A completed future
            // lets the contiguous frontier advance past the poison record.
            // A dead-letter policy is a planned follow-up.
            log.error("Failed to deserialize event from topic: {}, partition: {}, offset: {}, key: {} — "
                            + "record is skipped and its offset will be committed",
                    record.topic(), record.partition(), record.offset(), record.key(), e);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            // Not a data-format problem (e.g. serializer configuration error):
            // do not silently drop the record — fail it so the partition is
            // blocked and the record is re-delivered after restart.
            log.error("Unexpected error while preparing event from topic: {}, partition: {}, offset: {}, "
                            + "key: {} — the partition will be blocked and the record re-delivered after restart",
                    record.topic(), record.partition(), record.offset(), record.key(), e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Delivers a deserialized event to the consumer.
     *
     * @param record        the Kafka record the event came from
     * @param event         the deserialized event
     * @param eventConsumer the event consumer
     * @return the processing future to track for offset commitment
     */
    private CompletableFuture<?> deliverEvent(ConsumerRecord<String, byte[]> record, Event event,
                                              AsyncEventConsumer eventConsumer) {
        String previousDeliveredFrom = MDC.get(MDC_DELIVERED_FROM);
        MDC.put(MDC_DELIVERED_FROM, name() + "-" + record.topic() + "-p" + record.partition());
        Map<String, String> context = MDC.getCopyOfContextMap();
        try {
            CompletableFuture<?> processing = Objects.requireNonNull(eventConsumer.accept(event),
                    "AsyncEventConsumer.accept() must not return null; return a completed future "
                            + "for synchronous processing");
            CompletableFuture<?> tracked = processing.whenComplete((result, error) -> {
                if (error != null) {
                    runWithMdc(context, () -> log.error(
                            "Event processing failed for topic: {}, partition: {}, offset: {} — "
                                    + "the record will be re-delivered after restart",
                            record.topic(), record.partition(), record.offset(), error));
                }
            });
            if (log.isDebugEnabled()) {
                String payloadType;
                if (event instanceof Envelope<?> envelope) {
                    payloadType = envelope.payload() == null
                            ? "null"
                            : envelope.payload().getClass().getSimpleName();
                } else {
                    payloadType = event.getClass().getSimpleName();
                }
                log.debug("Event delivered from Kafka topic: {}, key: {}, event type: {}, payload type: {}",
                        record.topic(), record.key(), event.type().getSimpleName(), payloadType);
            }
            return tracked;
        } catch (Exception e) {
            // Synchronous consumer failure: the record must be re-delivered,
            // so its offset must not advance.
            log.error("Event delivery failed for topic: {}, partition: {}, offset: {} — "
                            + "offset will not be committed; the record will be re-delivered after restart",
                    record.topic(), record.partition(), record.offset(), e);
            return CompletableFuture.failedFuture(e);
        } finally {
            restoreMdc(previousDeliveredFrom);
        }
    }

    /**
     * Runs the action with the captured MDC context so asynchronous callbacks
     * keep the delivery diagnostics.
     *
     * @param context captured MDC context, may be null
     * @param action  the action to run
     */
    private static void runWithMdc(Map<String, String> context, Runnable action) {
        Map<String, String> previous = MDC.getCopyOfContextMap();
        try {
            if (context != null) {
                MDC.setContextMap(context);
            } else {
                MDC.clear();
            }
            action.run();
        } finally {
            if (previous != null) {
                MDC.setContextMap(previous);
            } else {
                MDC.clear();
            }
        }
    }

    /**
     * Restores the previous value of the delivery MDC key.
     *
     * @param previousDeliveredFrom the previous value, or null when absent
     */
    private static void restoreMdc(String previousDeliveredFrom) {
        if (previousDeliveredFrom != null) {
            MDC.put(MDC_DELIVERED_FROM, previousDeliveredFrom);
        } else {
            MDC.remove(MDC_DELIVERED_FROM);
        }
    }

    /**
     * Builds the rebalance listener that persists completed progress before a
     * partition is revoked and drops in-memory state for revoked, assigned and
     * lost partitions. Callbacks run on the consume-loop (poll) thread.
     *
     * @param consumer the Kafka consumer
     * @return the rebalance listener
     */
    private ConsumerRebalanceListener rebalanceListener(Consumer<String, byte[]> consumer) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitRevokedPartitions(consumer, partitions);
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
        };
    }

    /**
     * Commits the contiguous completed frontier of partitions about to be
     * revoked, so the new owner continues from the acknowledged position.
     *
     * @param consumer   the Kafka consumer
     * @param partitions revoked partitions
     */
    private void commitRevokedPartitions(Consumer<String, byte[]> consumer,
                                         Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> frontiers = new HashMap<>();
        for (TopicPartition partition : partitions) {
            TreeMap<Long, CompletableFuture<?>> pending = pendingByPartition.get(partition);
            if (pending == null) {
                continue;
            }
            // Ownership is moving: a failure is the new owner's concern, so do
            // not mark the partition blocked or log an error here.
            Long frontier = completedFrontierOnly(pending);
            Long committed = committedFrontiers.get(partition);
            if (frontier != null && (committed == null || frontier > committed)) {
                frontiers.put(partition, frontier);
            }
        }
        commitFrontiers(consumer, frontiers,
                "commit during revocation failed; the new owner will re-deliver uncommitted records");
    }

    /**
     * Drops all in-memory offset tracking for the given partitions.
     *
     * @param partitions partitions to forget
     */
    private void clearPartitionState(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            pendingByPartition.remove(partition);
            inFlightByPartition.remove(partition);
            committedFrontiers.remove(partition);
            blockedFrontiers.remove(partition);
            pausedPartitions.remove(partition);
        }
    }

    /**
     * Pauses partitions that are blocked by a failure, have too many in-flight
     * records, or have reached the hard buffer cap.
     *
     * @param consumer the Kafka consumer
     */
    private void applyBackpressure(Consumer<String, byte[]> consumer) {
        List<TopicPartition> toPause = new ArrayList<>();
        for (TopicPartition partition : pendingByPartition.keySet()) {
            if (pausedPartitions.contains(partition)) {
                continue;
            }
            TreeMap<Long, CompletableFuture<?>> pending = pendingByPartition.get(partition);
            int buffered = pending == null ? 0 : pending.size();
            boolean blocked = blockedFrontiers.containsKey(partition);
            boolean tooManyInFlight = inFlightCount(partition) >= maxInFlightRecordsPerPartition;
            boolean bufferFull = buffered >= maxBufferedRecordsPerPartition;
            if (blocked || tooManyInFlight || bufferFull) {
                toPause.add(partition);
            }
        }
        if (toPause.isEmpty()) {
            return;
        }
        try {
            consumer.pause(toPause);
            pausedPartitions.addAll(toPause);
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
     * have dropped below the limits. Blocked partitions are never resumed
     * automatically.
     *
     * @param consumer the Kafka consumer
     */
    private void releaseBackpressure(Consumer<String, byte[]> consumer) {
        Set<TopicPartition> assigned = assignedPartitions(consumer);
        List<TopicPartition> toResume = new ArrayList<>();
        for (TopicPartition partition : pausedPartitions) {
            if (blockedFrontiers.containsKey(partition) || !assigned.contains(partition)) {
                continue;
            }
            TreeMap<Long, CompletableFuture<?>> pending = pendingByPartition.get(partition);
            int buffered = pending == null ? 0 : pending.size();
            if (inFlightCount(partition) < maxInFlightRecordsPerPartition
                    && buffered < maxBufferedRecordsPerPartition) {
                toResume.add(partition);
            }
        }
        if (toResume.isEmpty()) {
            return;
        }
        try {
            consumer.resume(toResume);
            toResume.forEach(pausedPartitions::remove);
            log.debug("Resumed partitions after backpressure: {}", toResume);
        } catch (RuntimeException e) {
            // resume() is a local operation; keep the paused bookkeeping and
            // retry on the next iteration instead of killing the loop.
            log.warn("Failed to resume partitions {} ({}: {})",
                    toResume, e.getClass().getSimpleName(), e.getMessage());
        }
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

    /**
     * Returns the current in-flight record count of a partition.
     *
     * @param partition the partition
     * @return number of not-yet-completed futures
     */
    private int inFlightCount(TopicPartition partition) {
        AtomicInteger counter = inFlightByPartition.get(partition);
        return counter == null ? 0 : counter.get();
    }

    private void tryCloseConsumer() {
        try {
            kafkaConsumer.close(CLOSE_TIMEOUT);
        } catch (Exception e) {
            log.warn("Error closing consumer", e);
        }
    }

    /**
     * Builds the default consumer properties for at-least-once delivery:
     * auto-commit disabled and {@code auto.offset.reset=earliest}.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId          consumer group ID
     * @return consumer properties
     */
    static Properties defaultConsumerProperties(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        applyAtLeastOnceDefaults(props);
        return props;
    }

    private static Consumer<String, byte[]> createConsumer(String bootstrapServers, String groupId) {
        return new KafkaConsumer<>(defaultConsumerProperties(bootstrapServers, groupId));
    }

    private static Consumer<String, byte[]> createConsumer(Properties properties, String groupId) {
        Properties props = new Properties();
        props.putAll(properties);
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        applyAtLeastOnceDefaults(props);
        return new KafkaConsumer<>(props);
    }

    /**
     * Applies the at-least-once defaults for values the caller did not set.
     * <p>
     * User-provided values always win: explicitly setting
     * {@code enable.auto.commit=true} or a different {@code auto.offset.reset}
     * disables the corresponding at-least-once guarantee, and the transport will
     * rely on the caller's offset management.
     *
     * @param props consumer properties to complete
     */
    private static void applyAtLeastOnceDefaults(Properties props) {
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST);
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.putIfAbsent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "524288");
        props.putIfAbsent(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
    }

    /**
     * Splits a comma-separated topic list, trimming each name and ignoring
     * blank entries.
     * <p>
     * The result may be empty for a blank input; the transport constructors
     * reject an empty topic list.
     *
     * @param topics comma-separated topics
     * @return list of topic names
     * @throws IllegalArgumentException if {@code topics} is null
     */
    static List<String> parseTopics(String topics) {
        if (topics == null) {
            throw new IllegalArgumentException("Topic list must not be null");
        }
        return Arrays.stream(topics.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .toList();
    }
}
