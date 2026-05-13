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
                .whenComplete((results, throwable) -> {
                    long durationMs = System.currentTimeMillis() - startTime;
                    if (mdcContext != null) {
                        MDC.setContextMap(mdcContext);
                    }
                    try {
                        if (throwable != null) {
                            log.error("Event dispatch failed for {}: {}", event, throwable.getMessage(), throwable);
                        } else {
                            logEvent(event, results, durationMs);
                        }
                    } catch (Exception e) {
                        log.error("Failed to log event dispatch for {}: {}", event, e.getMessage(), e);
                    } finally {
                        MDC.clear();
                    }
                });
    }

    private void logEvent(Event event, HandlerResults results, long durationMs) {
        String json = buildLogEntry(event, results, durationMs);
        log.info(json);
    }

    private String buildLogEntry(Event event, HandlerResults results, long durationMs) {
        Map<String, Object> eventInfo = new LinkedHashMap<>();
        buildStatus(eventInfo, results);
        buildEventId(eventInfo, event);
        buildPayload(eventInfo, event);
        buildEnvelopeMetadata(eventInfo, event);
        buildHandlers(eventInfo, results);
        eventInfo.put("durationMs", durationMs);

        Map<String, Object> entry = new LinkedHashMap<>();
        addIfPresent(entry, "traceId", MDC.get("traceId"));
        addIfPresent(entry, "spanId", MDC.get("spanId"));
        entry.put("event", eventInfo);
        entry.put("@timestamp", Instant.now().toString());

        return toJson(entry);
    }

    private void buildStatus(Map<String, Object> eventInfo, HandlerResults results) {
        if (results != null && !results.isEmpty()) {
            if (results.isAllSuccess()) {
                eventInfo.put("status", "handled");
            } else if (results.isPartialSuccess()) {
                eventInfo.put("status", "partial");
            } else {
                eventInfo.put("status", "failed");
            }
        } else {
            eventInfo.put("status", "handled");
        }
    }

    private void buildEnvelopeMetadata(Map<String, Object> eventInfo, Event event) {
        if (event instanceof TraceableEvent te) {
            addIfPresent(eventInfo, "processId", te.processId());
            addIfPresent(eventInfo, "occurredAt", te.occurredAt());
        }
    }

    private void buildHandlers(Map<String, Object> eventInfo, HandlerResults results) {
        if (results == null || results.isEmpty()) {
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