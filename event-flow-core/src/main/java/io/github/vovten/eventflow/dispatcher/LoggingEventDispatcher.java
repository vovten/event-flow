package io.github.vovten.eventflow.dispatcher;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
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
import java.util.function.Consumer;

/**
 * Decorator for {@link EventDispatcher} that adds structured logging when events are dispatched.
 * <p>
 * Logs event handling information with machine-parseable JSON format:
 * <ul>
 *   <li>status - handling result (handled/partial/failed)</li>
 *   <li>eventId - envelope identifier</li>
 *   <li>payload - event data with type discriminator and fields</li>
 *   <li>occurredAt - event occurrence timestamp</li>
 *   <li>processId - process correlation identifier</li>
 *   <li>handlers - list of successful handler names</li>
 *   <li>failedHandlers - list of failed handler names with errors</li>
 *   <li>failedCount - number of failed handlers</li>
 *   <li>durationMs - processing duration in milliseconds</li>
 *   <li>traceId - distributed trace ID from MDC</li>
 *   <li>spanId - span ID from MDC</li>
 * </ul>
 * <p>
 * Logging is performed after the dispatch operation completes.
 * MDC context is preserved across async boundaries.
 *
 * @author Vladimir Aleshkov
 * @see IdempotentEventDispatcher
 * @since 2026-05-11
 */
public class LoggingEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventDispatcher.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final EventDispatcher origin;
    private final int maxPayloadLength;

    /**
     * Create logging decorator with default max payload length (500).
     *
     * @param origin the delegate dispatcher to wrap
     * @throws IllegalArgumentException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin) {
        this(origin, 500);
    }

    /**
     * Create logging decorator with custom max payload length.
     *
     * @param origin the delegate dispatcher to wrap
     * @param maxPayloadLength maximum length of payload in log output
     * @throws IllegalArgumentException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin, int maxPayloadLength) {
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.maxPayloadLength = maxPayloadLength;
    }

    @Override
    public CompletableFuture<HandlerResults> dispatch(Event event) {
        long startTime = System.currentTimeMillis();
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();

        return origin.dispatch(event)
                .whenComplete((results, error) -> {
                    long durationMs = System.currentTimeMillis() - startTime;
                    logEvent(event, results, error, durationMs, mdcContext);
                });
    }

    private void logEvent(Event event, HandlerResults results, Throwable error,
                          long durationMs, Map<String, String> mdcContext) {
        String json = buildLogEntry(event, results, error, durationMs, mdcContext);

        if (error != null || (results != null && results.isAllFailure())) {
            log.error(json);
        } else if (results != null && results.isPartialSuccess()) {
            log.warn(json);
        } else if (results != null && results.isEmpty()) {
            log.warn(json);
        } else {
            log.info(json);
        }
    }

    private String buildLogEntry(Event event, HandlerResults results, Throwable error,
                                 long durationMs, Map<String, String> mdcContext) {
        Map<String, Object> eventInfo = new LinkedHashMap<>();
        buildStatus(eventInfo, results, error);
        buildEventId(eventInfo, event);
        buildPayload(eventInfo, event);
        buildEnvelopeMetadata(eventInfo, event);
        buildHandlers(eventInfo, results);
        buildErrorInfo(eventInfo, error);

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("event", eventInfo);
        buildTracingContext(entry, mdcContext);
        eventInfo.put("durationMs", durationMs);
        entry.put("@timestamp", Instant.now().toString());

        return toJson(entry);
    }

    private void buildTracingContext(Map<String, Object> entry, Map<String, String> mdcContext) {
        if (mdcContext != null) {
            addIfPresent(entry, "traceId", mdcContext.get("traceId"));
            addIfPresent(entry, "spanId", mdcContext.get("spanId"));
            addIfPresent(entry, "deliveredFrom", mdcContext.get("deliveredFrom"));
        }
    }

    private void buildStatus(Map<String, Object> eventInfo, HandlerResults results, Throwable error) {
        if (error != null) {
            eventInfo.put("status", "failed");
        } else if (results != null && !results.isEmpty()) {
            if (results.isAllSuccess()) {
                eventInfo.put("status", "handled");
            } else if (results.isPartialSuccess()) {
                eventInfo.put("status", "partial");
            } else {
                eventInfo.put("status", "failed");
            }
        } else {
            eventInfo.put("status", "skipped");
        }
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

    private void buildEnvelopeMetadata(Map<String, Object> eventInfo, Event event) {
        if (event instanceof TraceableEvent te) {
            addIfPresent(eventInfo, "processId", te.processId());
            addIfPresent(eventInfo, "occurredAt", te.occurredAt());
        }
    }

    private void buildHandlers(Map<String, Object> eventInfo, HandlerResults results) {
        if (results == null || results.isEmpty()) {
            eventInfo.put("handlers", List.of());
            return;
        }
        List<String> handlerNames = results.getSuccesses().stream()
                .map(HandlerResult::handlerName)
                .toList();
        if (!handlerNames.isEmpty()) {
            eventInfo.put("handlers", handlerNames);
        }

        if (results.getFailedCount() > 0) {
            List<String> failedHandlers = results.getFailures().stream()
                    .map(f -> f.handlerName() + ": " + f.errorDetails())
                    .toList();
            eventInfo.put("failedHandlers", failedHandlers);
            eventInfo.put("failedCount", results.getFailedCount());
        }
    }

    private void buildEventId(Map<String, Object> eventInfo, Event event) {
        if (event instanceof TraceableEvent te && te.eventId() != null) {
            eventInfo.put("eventId", te.eventId().toString());
        }
    }

    private void buildPayload(Map<String, Object> eventInfo, Event event) {
        Object payload = extractPayload(event);
        eventInfo.put("payload", processPayload(payload));
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

    @Override
    public void register(Object listener) {
        origin.register(listener);
    }

    @Override
    public boolean isRegistered(Object listener) {
        return origin.isRegistered(listener);
    }

    @Override
    public void start(Consumer<Event> dispatchConsumer) {
        origin.start(dispatchConsumer);
    }

    @Override
    public void stop() {
        origin.stop();
    }
}