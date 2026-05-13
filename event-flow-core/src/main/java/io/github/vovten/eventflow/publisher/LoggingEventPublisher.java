package io.github.vovten.eventflow.publisher;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.util.EventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Decorator for {@link EventPublisher} that adds structured logging when events are published.
 * <p>
 * Logs event publishing information with machine-parseable JSON format:
 * <ul>
 *   <li>eventId - envelope identifier</li>
 *   <li>processId - process correlation identifier</li>
 *   <li>occurredAt - event timestamp</li>
 *   <li>channels - target channel names</li>
 *   <li>payload - event data with type discriminator and fields</li>
 *   <li>status - publication result (published/partial/failed)</li>
 *   <li>deliveredTo - list of successfully delivered destinations</li>
 *   <li>failedOn - list of failed destinations (if partial/failure)</li>
 *   <li>traceId - distributed trace ID from MDC</li>
 *   <li>spanId - span ID from MDC</li>
 * </ul>
 * <p>
 * Logging is performed asynchronously after the publish operation completes,
 * allowing capture of the actual result status and channel delivery details.
 * MDC context is preserved across async boundaries.
 *
 * @author Vladimir Aleshkov
 * @see ChannelEventPublisher
 * @see RetryEventPublisher
 * @since 2026-05-11
 */
public class LoggingEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventPublisher.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final EventPublisher origin;
    private final int maxPayloadLength;

    /**
     * Create logging decorator with default max payload length (500).
     *
     * @param origin the delegate publisher to wrap
     * @throws IllegalArgumentException if origin is null
     */
    public LoggingEventPublisher(EventPublisher origin) {
        this(origin, 500);
    }

    /**
     * Create logging decorator with custom max payload length.
     *
     * @param origin the delegate publisher to wrap
     * @param maxPayloadLength maximum length of payload in log output
     * @throws IllegalArgumentException if origin is null
     */
    public LoggingEventPublisher(EventPublisher origin, int maxPayloadLength) {
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.maxPayloadLength = maxPayloadLength;
    }

    @Override
    public CompletableFuture<SendResults> publish(Event event) {
        Instant start = Instant.now();
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();

        return origin.publish(event)
                .whenComplete((result, error) -> {
                    if (mdcContext != null) {
                        MDC.setContextMap(mdcContext);
                    }
                    try {
                        logEvent(event, result, error, start);
                    } finally {
                        MDC.clear();
                    }
                });
    }

    private void logEvent(Event event, SendResults result, Throwable error, Instant start) {
        String json = buildLogEntry(event, result, error, start);

        if (error != null) {
            log.error(json);
        } else if (result != null && result.isPartialSuccess()) {
            log.warn(json);
        } else if (result != null && result.isAllSuccess()) {
            log.info(json);
        } else if (result != null && result.isAllFailure()) {
            log.error(json);
        } else {
            log.info(json);
        }
    }

    private String buildLogEntry(Event event, SendResults result, Throwable error, Instant start) {
        Map<String, Object> entry = new LinkedHashMap<>();
        buildTracingContext(entry);
        Map<String, Object> eventInfo = new LinkedHashMap<>();
        buildStatus(eventInfo, result, error);
        buildEventId(eventInfo, event);
        buildPayload(eventInfo, event);
        buildEnvelopeMetadata(eventInfo, event);
        buildChannels(eventInfo, event);
        buildDeliveryInfo(eventInfo, result);
        buildErrorInfo(eventInfo, error);
        entry.put("event", eventInfo);
        entry.put("@timestamp", start.toString());
        return toJson(entry);
    }

    private void buildTracingContext(Map<String, Object> entry) {
        addIfPresent(entry, "traceId", MDC.get("traceId"));
        addIfPresent(entry, "spanId", MDC.get("spanId"));
    }

    private void buildStatus(Map<String, Object> eventInfo, SendResults result, Throwable error) {
        if (error != null) {
            eventInfo.put("status", "failed");
        } else if (result != null && result.isAllSuccess()) {
            eventInfo.put("status", "published");
        } else if (result != null && result.isPartialSuccess()) {
            eventInfo.put("status", "partial");
        } else if (result != null && result.isAllFailure()) {
            eventInfo.put("status", "failed");
        } else {
            eventInfo.put("status", "unknown");
        }
    }

    private void buildEventId(Map<String, Object> eventInfo, Event event) {
        if (event instanceof TraceableEvent te && te.eventId() != null) {
            eventInfo.put("eventId", te.eventId().toString());
        }
    }

    private void buildEnvelopeMetadata(Map<String, Object> eventInfo, Event event) {
        if (event instanceof TraceableEvent te) {
            addIfPresent(eventInfo, "processId", te.processId());
            addIfPresent(eventInfo, "occurredAt", te.occurredAt());
        }
    }

    private void buildChannels(Map<String, Object> eventInfo, Event event) {
        List<String> channelNames = event.channels().isEmpty()
                ? List.of()
                : event.channels().stream()
                        .map(Class::getSimpleName)
                        .collect(Collectors.toList());
        eventInfo.put("channels", channelNames);
    }

    private void buildDeliveryInfo(Map<String, Object> eventInfo, SendResults result) {
        if (result == null || result.isEmpty()) {
            return;
        }
        List<String> deliveredTo = result.getSuccesses().stream()
                .map(SendResult::destination)
                .collect(Collectors.toList());
        eventInfo.put("deliveredTo", deliveredTo);

        if (result.isPartialSuccess() || result.isAllFailure()) {
            List<String> failedOn = result.getFailures().stream()
                    .map(f -> f.destination() + ": " + f.errorDetails())
                    .collect(Collectors.toList());
            eventInfo.put("failedOn", failedOn);
        }
    }

    private void buildPayload(Map<String, Object> eventInfo, Event event) {
        Object payload = extractPayload(event);
        eventInfo.put("payload", processPayload(payload));
    }

    private void buildErrorInfo(Map<String, Object> eventInfo, Throwable error) {
        if (error == null) {
            return;
        }
        Map<String, Object> errorInfo = new LinkedHashMap<>();
        errorInfo.put("message", error.getMessage());
        errorInfo.put("type", error.getClass().getSimpleName());
        eventInfo.put("error", errorInfo);
    }

    private Object extractPayload(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload();
        }
        return event;
    }

    private Object processPayload(Object payload) {
        if (payload == null) {
            return null;
        }
        Map<String, Object> payloadInfo = new LinkedHashMap<>();
        payloadInfo.put("@class", payload.getClass().getName());
        try {
            String json = EventUtils.toJson(payload);

            if (json.length() > maxPayloadLength) {
                payloadInfo.put("_truncated", true);
                payloadInfo.put("_originalSize", json.length());
                String truncated = json.substring(0, maxPayloadLength) + "...";
                if (!addPayloadFields(payloadInfo, truncated)) {
                    payloadInfo.put("data", truncated);
                }
            } else {
                if (!addPayloadFields(payloadInfo, json)) {
                    payloadInfo.put("data", json);
                }
            }
        } catch (Exception e) {
            payloadInfo.put("_raw", payload.toString());
        }
        return payloadInfo;
    }

    private boolean addPayloadFields(Map<String, Object> payloadInfo, String json) {
        Map<String, Object> fields = parseJsonToMap(json);
        if (fields != null) {
            fields.forEach((key, value) -> {
                if (!"@class".equals(key)) {
                    payloadInfo.put(key, value);
                }
            });
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonToMap(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            return null;
        }
    }

    private void addIfPresent(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value instanceof UUID uuid ? uuid.toString() : value);
        }
    }

    private String toJson(Object obj) {
        try {
            return EventUtils.toJson(obj);
        } catch (Exception e) {
            return "{\"error\":\"Failed to serialize log: " + e.getMessage() + "\"}";
        }
    }
}