package io.github.vovten.eventflow.transport;

import java.time.Instant;
import java.util.Map;

/**
 * Result of an async event send operation.
 *
 * @param success       true if the send was successful
 * @param destination   destination name (e.g., "kafka-topic-p0", "local-queue")
 * @param timestamp     when the send completed
 * @param messageId     optional message ID
 * @param metadata      transport-specific data (e.g., Kafka offset, partition)
 * @param error         exception if send failed, null otherwise
 * @param errorDetails  error message if send failed, null otherwise
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public record SendResult(
        boolean success,
        String destination,
        Instant timestamp,
        String messageId,
        Map<String, Object> metadata,
        Throwable error,
        String errorDetails
) {

    /** Successful send result. */
    public static SendResult success(String destination) {
        return new SendResult(true, destination, Instant.now(), null, Map.of(), null, null);
    }

    /** Successful send result with metadata. */
    public static SendResult success(String destination, Map<String, Object> metadata) {
        return new SendResult(true, destination, Instant.now(), null,
                metadata != null ? Map.copyOf(metadata) : Map.of(), null, null);
    }

    /** Successful send result with message ID. */
    public static SendResult success(String destination, String messageId) {
        return new SendResult(true, destination, Instant.now(), messageId, Map.of(), null, null);
    }

    /** Successful send result with message ID and metadata. */
    public static SendResult success(String destination, String messageId, Map<String, Object> metadata) {
        return new SendResult(true, destination, Instant.now(), messageId,
                metadata != null ? Map.copyOf(metadata) : Map.of(), null, null);
    }

    /** Failed send result with exception. */
    public static SendResult failure(String destination, Throwable error, String errorDetails) {
        return new SendResult(false, destination, Instant.now(), null, Map.of(), error, errorDetails);
    }

    /** Failed send result with exception and metadata. */
    public static SendResult failure(String destination, Throwable error, Map<String, Object> metadata) {
        return new SendResult(false, destination, Instant.now(), null,
                metadata != null ? Map.copyOf(metadata) : Map.of(), error, error != null ? error.getMessage() : null);
    }

    /** Failed send result without exception. */
    public static SendResult failure(String destination, String errorDetails) {
        return new SendResult(false, destination, Instant.now(), null, Map.of(), null, errorDetails);
    }
}
