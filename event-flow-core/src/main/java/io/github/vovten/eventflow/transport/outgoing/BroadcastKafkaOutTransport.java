package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.TransportException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Kafka transport that sends events to all topic partitions.
 * <p>
 * The record key does not influence partition routing here (every partition is
 * targeted explicitly); it is only written into the record metadata. The default
 * key strategy is {@link EventKeyResolver#eventType()}.
 * <p>
 * The broadcast semantics are strict: if <b>any</b> partition fails, the whole
 * send is reported as failed and contains the successful/failed partition lists
 * in the result metadata. Reporting partial delivery as success would silently
 * lose the event for subscribers of the failed partitions; with a failure result
 * the publisher's retry path re-delivers the event (successful partitions receive
 * a duplicate, which the at-least-once contract allows).
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class BroadcastKafkaOutTransport extends AbstractKafkaOutTransport {

    private static final Logger log = LoggerFactory.getLogger(BroadcastKafkaOutTransport.class);

    /**
     * How long the partition metadata is reused before it is fetched again.
     * Partition changes are rare, and a metadata request on every send adds a
     * blocking round-trip (up to {@code max.block.ms}).
     */
    static final Duration PARTITION_CACHE_TTL = Duration.ofMinutes(5);

    private volatile List<PartitionInfo> cachedPartitions;
    private volatile Instant partitionsFetchedAt;

    /**
     * Create broadcast Kafka transport with custom configuration.
     *
     * @param properties Kafka producer configuration
     * @param topic      Kafka topic name
     */
    public BroadcastKafkaOutTransport(Properties properties, String topic) {
        this(properties, topic, new JsonEventSerializer(), EventKeyResolver.eventType());
    }

    /**
     * Create broadcast Kafka transport with custom configuration and serializer.
     *
     * @param properties Kafka producer configuration
     * @param topic      Kafka topic name
     * @param serializer the event serializer to use
     */
    public BroadcastKafkaOutTransport(Properties properties, String topic, EventSerializer serializer) {
        this(properties, topic, serializer, EventKeyResolver.eventType());
    }

    /**
     * Create broadcast Kafka transport with custom configuration, serializer and key resolver.
     *
     * @param properties  Kafka producer configuration
     * @param topic       Kafka topic name
     * @param serializer  the event serializer to use
     * @param keyResolver key resolver strategy (used only for the record key, not routing)
     */
    public BroadcastKafkaOutTransport(Properties properties, String topic, EventSerializer serializer,
                                      EventKeyResolver keyResolver) {
        super(properties, topic, serializer, keyResolver);
    }

    /**
     * Create broadcast Kafka transport with bootstrap servers and topic.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     */
    public BroadcastKafkaOutTransport(String bootstrapServers, String topic) {
        this(createDefaultProperties(bootstrapServers), topic, new JsonEventSerializer(), EventKeyResolver.eventType());
    }

    /**
     * Create broadcast Kafka transport with bootstrap servers, topic, and custom serializer.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     * @param serializer       custom event serializer
     */
    public BroadcastKafkaOutTransport(String bootstrapServers, String topic, EventSerializer serializer) {
        this(createDefaultProperties(bootstrapServers), topic, serializer, EventKeyResolver.eventType());
    }

    /**
     * Create broadcast Kafka transport with bootstrap servers, topic, serializer and key resolver.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     * @param serializer       custom event serializer
     * @param keyResolver      key resolver strategy (used only for the record key, not routing)
     */
    public BroadcastKafkaOutTransport(String bootstrapServers, String topic, EventSerializer serializer,
                                      EventKeyResolver keyResolver) {
        super(createDefaultProperties(bootstrapServers), topic, serializer, keyResolver);
    }

    private static Properties createDefaultProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    /**
     * Send event to all partitions of the Kafka topic.
     * <p>
     * This method retrieves partition metadata and sends the event to each partition.
     * The semantics are strict: a failure in any partition fails the whole
     * broadcast. The result then carries the failure cause, a detailed message
     * and the successful/failed partition lists in its metadata.
     * A failure to resolve the key, serialize the event or enumerate partitions
     * (e.g., when the topic has no partitions) is surfaced through the returned
     * future, consistent with the base {@link #send(Event)} contract.
     *
     * @param event the event to send
     * @param key   the Kafka record key (does not influence routing, partitions are explicit)
     * @param value the serialized event payload
     * @return CompletableFuture with the send result: successful only when every
     *         partition accepted the event
     */
    @Override
    protected CompletableFuture<SendResult> dispatch(Event event, String key, byte[] value) {
        List<PartitionInfo> partitions = partitions();
        if (partitions.isEmpty()) {
            throw new TransportException(
                    String.format("Topic '%s' has no partitions for event %s", topic, event.type().getSimpleName())
            );
        }
        List<CompletableFuture<PartitionSendResult>> futures = new ArrayList<>();

        for (PartitionInfo partition : partitions) {
            int partitionId = partition.partition();
            CompletableFuture<SendResult> sendFuture = new CompletableFuture<>();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partitionId, key, value);
            producer.send(record, createSendCallback(sendFuture));
            CompletableFuture<PartitionSendResult> partitionFuture = sendFuture
                    .thenApply(result -> new PartitionSendResult(partitionId, result, null))
                    .exceptionally(ex -> new PartitionSendResult(partitionId, null, ex));
            futures.add(partitionFuture);
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> applyBroadcastStrategy(collectResults(futures), event, partitions.size()));
    }

    /**
     * Returns the topic partitions, reusing the cached metadata while it is fresh.
     * <p>
     * {@code KafkaProducer.partitionsFor} is a blocking metadata request; caching
     * keeps the broadcast path free of a network round-trip per event. An empty
     * result is not cached so a topic created later becomes visible immediately.
     * Partition changes are picked up after {@link #PARTITION_CACHE_TTL} or after
     * an explicit {@link #invalidatePartitionCache()}.
     *
     * @return the current partitions of the topic
     */
    private List<PartitionInfo> partitions() {
        List<PartitionInfo> cached = cachedPartitions;
        Instant fetchedAt = partitionsFetchedAt;
        if (cached != null && fetchedAt != null
                && Duration.between(fetchedAt, Instant.now()).compareTo(PARTITION_CACHE_TTL) < 0) {
            return cached;
        }
        List<PartitionInfo> fetched = producer.partitionsFor(topic);
        if (!fetched.isEmpty()) {
            cachedPartitions = List.copyOf(fetched);
            partitionsFetchedAt = Instant.now();
        }
        return fetched;
    }

    /**
     * Drops the cached partition metadata so the next send re-reads it.
     * <p>
     * Package-private: used by tests and by callers that need to react to a
     * partition change before the cache TTL expires.
     */
    void invalidatePartitionCache() {
        cachedPartitions = null;
        partitionsFetchedAt = null;
    }

    private SendResult applyBroadcastStrategy(
            List<PartitionSendResult> results,
            Event event,
            int totalPartitions
    ) {
        List<PartitionSendResult> failedPartitions = extractFailedPartitions(results);
        if (failedPartitions.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Successfully broadcast event {} to all {} partitions of topic '{}'",
                        event.type().getSimpleName(), totalPartitions, topic
                );
            }
            return findFirstSuccessfulResult(results, topic);
        }

        List<Integer> successfulPartitions = extractSuccessfulPartitions(results);
        List<Integer> failedPartitionIds = failedPartitions.stream()
                .map(PartitionSendResult::partitionId)
                .toList();
        String errorMessage = buildFailureMessage(event, totalPartitions, successfulPartitions, failedPartitions);
        log.warn(errorMessage);

        PartitionSendResult firstFailure = failedPartitions.getFirst();
        Throwable cause = firstFailure.exception() != null
                ? firstFailure.exception()
                : firstFailure.sendResult().error();
        Map<String, Object> metadata = Map.of(
                "totalPartitions", totalPartitions,
                "successfulPartitions", successfulPartitions,
                "failedPartitions", failedPartitionIds
        );
        return SendResult.failure(topic, cause, errorMessage, metadata);
    }

    private List<PartitionSendResult> collectResults(
            List<CompletableFuture<PartitionSendResult>> futures
    ) {
        return futures.stream()
                .map(CompletableFuture::join)
                .toList();
    }

    private List<Integer> extractSuccessfulPartitions(List<PartitionSendResult> results) {
        return results.stream()
                .filter(r -> r.sendResult() != null && r.sendResult().success())
                .map(PartitionSendResult::partitionId)
                .toList();
    }

    private List<PartitionSendResult> extractFailedPartitions(List<PartitionSendResult> results) {
        return results.stream()
                .filter(r -> r.exception() != null || (r.sendResult() != null && !r.sendResult().success()))
                .toList();
    }

    private SendResult findFirstSuccessfulResult(List<PartitionSendResult> results, String topic) {
        return results.stream()
                .map(PartitionSendResult::sendResult)
                .filter(r -> r != null && r.success())
                .findFirst()
                .orElse(SendResult.failure(topic, "All partitions failed"));
    }

    private String buildFailureMessage(
            Event event,
            int totalPartitions,
            List<Integer> successfulPartitions,
            List<PartitionSendResult> failedPartitions
    ) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Failed to broadcast event ")
                .append(event.type().getSimpleName())
                .append(" to topic '")
                .append(topic)
                .append("' (")
                .append(successfulPartitions.size())
                .append("/")
                .append(totalPartitions)
                .append(" successful)");
        if (!successfulPartitions.isEmpty()) {
            sb.append(". Successful partitions: ").append(successfulPartitions);
        }
        sb.append(". Failures:");
        for (PartitionSendResult result : failedPartitions) {
            String error = result.exception() != null
                    ? result.exception().getMessage()
                    : result.sendResult().errorDetails();
            sb.append(" [partition=")
                    .append(result.partitionId())
                    .append(": ")
                    .append(error)
                    .append("]");
        }
        return sb.toString();
    }

    /**
     * Holds the result of sending an event to a single partition.
     */
    protected record PartitionSendResult(int partitionId, SendResult sendResult, Throwable exception) {
    }
}