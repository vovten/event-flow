package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
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

/**
 * Kafka transport that sends events to all topic partitions.
 * <p>
 * If all partitions fail, the result contains the error.
 * If some partitions fail, a warning is logged and the first successful result is returned.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class BroadcastKafkaOutTransport extends KafkaOutTransport {

    private static final Logger log = LoggerFactory.getLogger(BroadcastKafkaOutTransport.class);

    /**
     * Create broadcast Kafka transport with custom configuration.
     *
     * @param properties Kafka producer configuration
     * @param topic      Kafka topic name
     */
    public BroadcastKafkaOutTransport(Properties properties, String topic) {
        super(properties, topic);
    }

    /**
     * Create broadcast Kafka transport with bootstrap servers and topic.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     */
    public BroadcastKafkaOutTransport(String bootstrapServers, String topic) {
        super(bootstrapServers, topic);
    }

    /**
     * Create broadcast Kafka transport with bootstrap servers, topic, and custom serializer.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param topic            Kafka topic name
     * @param serializer       custom event serializer
     */
    public BroadcastKafkaOutTransport(String bootstrapServers, String topic, EventSerializer serializer) {
        super(bootstrapServers, topic, serializer);
    }

    /**
     * Send event to all partitions of the Kafka topic.
     * <p>
     * This method retrieves partition metadata and sends the event to each partition.
     * It tracks successes and failures to provide detailed error handling:
     * <ul>
     *   <li>If all sends fail — throws an exception</li>
     *   <li>If some sends fail — logs a warning with partition details</li>
     * </ul>
     *
     * @param event the event to send
     * @return CompletableFuture with SendResult from the first successful partition
     */
    @Override
    public CompletableFuture<SendResult> send(Event event) {
        List<PartitionInfo> partitions = producer.partitionsFor(topic);
        if (partitions.isEmpty()) {
            throw new TransportException(
                    String.format("Topic '%s' has no partitions for event %s", topic, event.type().getSimpleName())
            );
        }
        String key = event.type().getName();
        byte[] value = serializer.serialize(event);
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
