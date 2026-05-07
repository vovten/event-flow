package io.github.vovten.eventflow.autoconfig.persistence;

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
import java.util.List;

/**
 * Scheduler for retrying failed events from the outbox table.
 * <p>
 * Periodically reads events where retry=true and status in (PENDING, FAILED),
 * then attempts to republish them.
 * <p>
 * Configuration:
 * <pre>{@code
 * event-flow:
 *   publisher:
 *     persistence:
 *       enabled: true
 *       retry:
 *         enabled: true
 *         fixed-delay: 30000  # milliseconds between retry attempts
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
    private final ObjectMapper objectMapper;
    private final int batchSize;

    public OutboxRetryScheduler(
            EventPublisher delegate,
            EventRepository repository,
            EventFlowProperties properties) {
        this(delegate, repository, 50);
    }

    public OutboxRetryScheduler(
            EventPublisher delegate,
            EventRepository repository,
            int batchSize) {
        this.delegate = delegate;
        this.repository = repository;
        this.batchSize = batchSize;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        log.info("OutboxRetryScheduler initialized with batchSize={}", batchSize);
    }

    @Scheduled(fixedDelayString = "${event-flow.publisher.persistence.retry.fixed-delay:30000}")
    public void retryFailedEvents() {
        log.debug("Checking for events to retry...");

        List<EventRecord> events = repository.findFailed(batchSize);
        if (events.isEmpty()) {
            log.debug("No events to retry");
            return;
        }

        log.info("Found {} events to retry", events.size());

        for (EventRecord record : events) {
            try {
                retryEvent(record);
            } catch (Exception e) {
                log.error("Failed to retry event: id={}", record.id(), e);
                repository.updateStatus(record.id(), EventStatus.FAILED, e.getMessage());
            }
        }
    }

    private void retryEvent(EventRecord record) throws Exception {
        log.debug("Retrying event: id={}", record.id());

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

        // Mark as published
        repository.updateStatus(record.id(), EventStatus.PUBLISHED);
        log.info("Successfully retried event: id={}", record.id());
    }
}