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

import java.util.ArrayList;
import java.util.List;
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
 * If all partitions fail, the result contains the error.
 * If some partitions fail, a warning is logged and the first successful result is returned.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class BroadcastKafkaOutTransport extends AbstractKafkaOutTransport {

    private static final Logger log = LoggerFactory.getLogger(BroadcastKafkaOutTransport.class);

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
     * It tracks successes and failures to provide detailed error handling:
     * <ul>
     *   <li>If all sends fail — the result contains the error</li>
     *   <li>If some sends fail — a warning is logged with partition details</li>
     * </ul>
     * A failure to resolve the key, serialize the event or enumerate partitions
     * (e.g., when the topic has no partitions) is surfaced through the returned
     * future, consistent with the base {@link #send(Event)} contract.
     *
     * @param event the event to send
     * @param key   the Kafka record key (does not influence routing, partitions are explicit)
     * @param value the serialized event payload
     * @return CompletableFuture with SendResult from the first successful partition
     */
    @Override
    protected CompletableFuture<SendResult> dispatch(Event event, String key, byte[] value) {
        List<PartitionInfo> partitions = producer.partitionsFor(topic);
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
            producer.send(record, createSendCallback(sendFuture, topic + "-p" + partitionId));
            CompletableFuture<PartitionSendResult> partitionFuture = sendFuture
                    .thenApply(result -> new PartitionSendResult(partitionId, result, null))
                    .exceptionally(ex -> new PartitionSendResult(partitionId, null, ex));
            futures.add(partitionFuture);
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> applyBroadcastStrategy(collectResults(futures), event, partitions.size()));
    }

    private SendResult applyBroadcastStrategy(
            List<PartitionSendResult> results,
            Event event,
            int totalPartitions
    ) {
        List<Integer> successfulPartitions = extractSuccessfulPartitions(results);
        List<PartitionSendResult> failedPartitions = extractFailedPartitions(results);

        if (successfulPartitions.isEmpty()) {
            String errorMessage = buildErrorMessage(event, totalPartitions, failedPartitions);
            PartitionSendResult firstFailure = failedPartitions.getFirst();
            Throwable cause = firstFailure.exception() != null
                    ? firstFailure.exception()
                    : firstFailure.sendResult().error();
            return SendResult.failure(topic, cause, errorMessage);
        }
        if (!failedPartitions.isEmpty()) {
            log.warn(buildWarningMessage(event, totalPartitions, successfulPartitions, failedPartitions));
        }
        if (log.isDebugEnabled() && successfulPartitions.size() == totalPartitions) {
            log.debug(
                    "Successfully broadcast event {} to all {} partitions of topic '{}'",
                    event.type().getSimpleName(), totalPartitions, topic
            );
        }
        return findFirstSuccessfulResult(results, topic);
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

    private String buildErrorMessage(
            Event event,
            int totalPartitions,
            List<PartitionSendResult> failedPartitions
    ) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Failed to broadcast event ")
                .append(event.type().getSimpleName())
                .append(" to any partition of topic '")
                .append(topic)
                .append("' (0/")
                .append(totalPartitions)
                .append(" successful)");
        if (!failedPartitions.isEmpty()) {
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
        }
        return sb.toString();
    }

    private String buildWarningMessage(
            Event event,
            int totalPartitions,
            List<Integer> successfulPartitions,
            List<PartitionSendResult> failedPartitions
    ) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Partial broadcast of event ")
                .append(event.type().getSimpleName())
                .append(" to topic '")
                .append(topic)
                .append("' (")
                .append(successfulPartitions.size())
                .append("/")
                .append(totalPartitions)
                .append(" successful). Successful partitions: ")
                .append(successfulPartitions)
                .append(". Failed partitions:");
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