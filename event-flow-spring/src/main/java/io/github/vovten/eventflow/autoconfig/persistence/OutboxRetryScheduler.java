package io.github.vovten.eventflow.autoconfig.persistence;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.persistence.EventRecord;
import io.github.vovten.eventflow.publisher.persistence.EventRepository;
import io.github.vovten.eventflow.publisher.persistence.EventStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Scheduler for retrying failed events from the outbox table.
 * <p>
 * Handles two types of retry:
 * <ul>
 *   <li>{@code retry=true} - manually marked events (for operator control)</li>
 *   <li>{@code status=FAILED} - automatic retry with exponential backoff</li>
 * </ul>
 * <p>
 * Configuration:
 * <pre>{@code
 * event-flow:
 *   publisher:
 *     persistence:
 *       enabled: true
 *       retry:
 *         enabled: true
 *         fixed-delay: 30000
 *         max-retry-count: 5
 *         initial-delay: 10000
 *         max-delay: 300000
 *         multiplier: 2.0
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
@ConditionalOnProperty(prefix = "event-flow.publisher.persistence.retry", name = "enabled", havingValue = "true")
public class OutboxRetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRetryScheduler.class);

    private final EventPublisher delegate;
    private final EventRepository repository;
    private final EventFlowProperties.OutboxRetryConfig config;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    public OutboxRetryScheduler(
            EventPublisher delegate,
            EventRepository repository,
            EventFlowProperties properties) {
        this(delegate, repository, properties.getPublisher().getPersistence().getRetry(), 50);
    }

    public OutboxRetryScheduler(
            EventPublisher delegate,
            EventRepository repository,
            EventFlowProperties.OutboxRetryConfig config,
            int batchSize) {
        this.delegate = delegate;
        this.repository = repository;
        this.config = config;
        this.batchSize = batchSize;
        this.objectMapper = createObjectMapper();
        log.info("OutboxRetryScheduler initialized with batchSize={}, maxRetryCount={}, initialDelay={}ms, maxDelay={}ms, multiplier={}",
                batchSize, config.getMaxRetryCount(), config.getInitialDelay(), config.getMaxDelay(), config.getMultiplier());
    }

    @Scheduled(fixedDelayString = "${event-flow.publisher.persistence.retry.fixed-delay:30000}")
    public void retryEvents() {
        log.debug("Checking for events to retry...");

        // 1. Process manually marked events (retry=true)
        processManualRetries();

        // 2. Process failed events with exponential backoff
        processFailedRetries();

        log.debug("Retry cycle completed");
    }

    /**
     * Process events with retry=true flag (manual/operator control).
     */
    private void processManualRetries() {
        List<EventRecord> events = repository.findFailed(batchSize);
        if (events.isEmpty()) {
            return;
        }

        log.info("Found {} manually marked events to retry", events.size());

        for (EventRecord record : events) {
            if (record.retry()) {
                publishEvent(record, true);
            } else {
                // Event was marked retry=false or status changed, skip
                log.debug("Skipping event (no longer marked for retry): id={}", record.id());
            }
        }
    }

    /**
     * Process failed events with exponential backoff.
     */
    private void processFailedRetries() {
        // Calculate the minimum modifiedAt time (oldest events to check)
        // We need events where enough time has passed since last attempt
        Instant minModifiedAt = Instant.now().minusMillis(config.getInitialDelay());
        
        List<EventRecord> events = repository.findFailedForRetry(
                batchSize, 
                config.getMaxRetryCount(),
                minModifiedAt
        );

        if (events.isEmpty()) {
            return;
        }

        log.info("Found {} failed events eligible for retry", events.size());

        for (EventRecord record : events) {
            // Calculate if enough time has passed based on retry count
            if (isDelayPassed(record)) {
                publishEvent(record, false);
            } else {
                log.trace("Event not ready for retry yet: id={}, retryCount={}, modifiedAt={}", 
                        record.id(), record.retryCount(), record.modifiedAt());
            }
        }
    }

    /**
     * Check if the backoff delay has passed for this event.
     */
    private boolean isDelayPassed(EventRecord record) {
        long delayMs = calculateDelay(record.retryCount());
        Instant readyAt = record.modifiedAt().plusMillis(delayMs);
        return Instant.now().isAfter(readyAt);
    }

    /**
     * Calculate delay for a given retry count using exponential backoff.
     * Formula: min(initialDelay * (multiplier ^ retryCount), maxDelay)
     */
    long calculateDelay(int retryCount) {
        if (retryCount <= 0) {
            return config.getInitialDelay();
        }
        
        double delay = config.getInitialDelay() * Math.pow(config.getMultiplier(), retryCount - 1);
        return Math.min((long) delay, config.getMaxDelay());
    }

    /**
     * Publish an event.
     * @param record the event record
     * @param clearRetryFlag if true, clears the retry flag after successful publish
     */
    private void publishEvent(EventRecord record, boolean clearRetryFlag) {
        try {
            log.debug("Publishing event: id={}, retryCount={}", record.id(), record.retryCount());

            // Deserialize JSON stored in event field
            String json = record.event();
            byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);

            // Try to parse as Envelope first (most common case)
            try {
                Envelope<?> envelope = objectMapper.readValue(jsonBytes, Envelope.class);
                delegate.publish(envelope);
            } catch (Exception e) {
                // Fallback: try to parse as generic object and publish
                log.warn("Failed to parse as Envelope, trying generic approach: id={}", record.id());
                Object event = objectMapper.readValue(jsonBytes, Object.class);
                delegate.publish(event);
            }

            // Mark as published and clear retry flag if needed
            if (clearRetryFlag) {
                repository.updateFields(record.id(), new EventRepository.FieldUpdate()
                        .status(EventStatus.PUBLISHED)
                        .retry(false)
                        .modifiedAt(Instant.now()));
            } else {
                repository.updateStatus(record.id(), EventStatus.PUBLISHED);
            }
            
            log.info("Successfully published event: id={}", record.id());

        } catch (Exception e) {
            log.error("Failed to publish event: id={}", record.id(), e);
            int newRetryCount = repository.markFailed(record.id(), e.getMessage());
            
            if (newRetryCount >= config.getMaxRetryCount()) {
                log.warn("Event exceeded max retry count: id={}, retryCount={}, maxRetryCount={}", 
                        record.id(), newRetryCount, config.getMaxRetryCount());
            }
        }
    }

    private static ObjectMapper createObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}