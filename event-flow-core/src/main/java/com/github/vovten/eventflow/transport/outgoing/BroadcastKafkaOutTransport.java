package com.github.vovten.eventflow.transport.outgoing;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.TransportException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Broadcast Kafka publisher transport for event delivery to all topic partitions.
 * <p>
 * This transport extends {@link KafkaOutTransport} to provide
 * broadcast functionality — it sends each event to <b>all partitions</b> of the topic.
 * This is useful when you need to ensure that all consumers across all partitions
 * receive the event.
 * <p>
 * <b>Error handling behavior:</b>
 * <ul>
 *   <li>If sending fails for all partitions — throws {@link TransportException}</li>
 *   <li>If sending fails for some partitions — logs a WARN message with details</li>
 *   <li>If sending succeeds for all partitions — event is delivered successfully</li>
 * </ul>
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Broadcast events that must be received by all consumers</li>
 *   <li>Configuration updates across microservices</li>
 *   <li>System-wide announcements</li>
 *   <li>Cache invalidation signals</li>
 * </ul>
 * <p>
 * <b>Configuration example:</b>
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 * PublisherTransport transport = new BroadcastKafkaPublisherTransport(props, "events");
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 * @see KafkaOutTransport
 */
public class BroadcastKafkaOutTransport extends KafkaOutTransport {

    private static final Logger logger = LoggerFactory.getLogger(BroadcastKafkaOutTransport.class);

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
     * Send event to all partitions of the Kafka topic.
     * <p>
     * This method retrieves partition metadata and sends the event to each partition.
     * It tracks successes and failures to provide detailed error handling:
     * <ul>
     *   <li>If all sends fail — throws an exception</li>
     *   <li>If some sends fail — logs a warning with partition details</li>
     * </ul>
     *
     * @param event                           the event to send
     * @throws TransportException if sending fails for all partitions
     */
    @Override
    public void send(Event event) {
        List<PartitionInfo> partitions = producer.partitionsFor(topic);
        if (partitions.isEmpty()) {
            throw new TransportException(
                    String.format("Topic '%s' has no partitions for event %s", topic, event.type().getSimpleName())
            );
        }
        String key = event.type().getName();
        String value = event.asJson();
        List<Integer> successfulPartitions = new ArrayList<>();
        List<PartitionSendResult> failedPartitions = new ArrayList<>();

        for (PartitionInfo partition : partitions) {
            int partitionId = partition.partition();
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionId, key, value);
                trySend(event, record);
                successfulPartitions.add(partitionId);
            } catch (TransportException e) {
                failedPartitions.add(new PartitionSendResult(partitionId, e));
            }
        }
        handleSendResults(event, partitions.size(), successfulPartitions, failedPartitions);
    }

    /**
     * Get partition information for the topic.
     *
     * @return list of partition info
     */
    protected List<PartitionInfo> getPartitions() {
        return producer.partitionsFor(topic);
    }

    /**
     * Handle send results and apply error handling logic.
     *
     * @param event                the event that was sent
     * @param totalPartitions      total number of partitions
     * @param successfulPartitions list of successfully sent partition IDs
     * @param failedPartitions     list of failed partition send results
     * @throws TransportException if all sends failed
     */
    protected void handleSendResults(
            Event event,
            int totalPartitions,
            List<Integer> successfulPartitions,
            List<PartitionSendResult> failedPartitions
    ) {
        int successCount = successfulPartitions.size();
        int failCount = failedPartitions.size();

        if (successCount == 0) {
            String errorMessage = buildErrorMessage(event, totalPartitions, failedPartitions);
            Throwable cause = failedPartitions.getFirst().exception();
            throw new TransportException(errorMessage, cause);
        }

        if (failCount > 0) {
            logger.warn(buildWarningMessage(event, totalPartitions, successfulPartitions, failedPartitions));
        }

        if (logger.isDebugEnabled() && successCount == totalPartitions) {
            logger.debug(
                    "Successfully broadcast event {} to all {} partitions of topic '{}'",
                    event.type().getSimpleName(), totalPartitions, topic
            );
        }
    }

    private String buildErrorMessage(
            Event event,
            int totalPartitions,
            List<PartitionSendResult> failedPartitions
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "Failed to broadcast event %s to any partition of topic '%s' (0/%d successful)",
                event.type().getSimpleName(), topic, totalPartitions
        ));

        if (!failedPartitions.isEmpty()) {
            sb.append(". Failures:");
            for (PartitionSendResult result : failedPartitions) {
                sb.append(String.format(
                        " [partition=%d: %s]",
                        result.partitionId(),
                        result.exception().getMessage()
                ));
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
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "Partial broadcast of event %s to topic '%s' (%d/%d successful)",
                event.type().getSimpleName(), topic, successfulPartitions.size(), totalPartitions
        ));
        sb.append(String.format(". Successful partitions: %s", successfulPartitions));
        sb.append(". Failed partitions:");
        for (PartitionSendResult result : failedPartitions) {
            sb.append(String.format(
                    " [partition=%d: %s]",
                    result.partitionId(),
                    result.exception().getMessage()
            ));
        }

        return sb.toString();
    }

    /**
     * Record holding partition send failure information.
     *
     * @param partitionId the partition ID that failed
     * @param exception   the exception that occurred
     */
    protected record PartitionSendResult(int partitionId, TransportException exception) {
    }
}
