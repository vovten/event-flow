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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Decorator for {@link EventDispatcher} that adds structured logging when events are dispatched.
 * <p>
 * Logs event handling information at INFO level with machine-parseable JSON format:
 * <ul>
 *   <li>eventId - envelope identifier</li>
 *   <li>handler - handler class name</li>
 *   <li>durationMs - processing duration in milliseconds</li>
 *   <li>payload - event data with type discriminator and fields</li>
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
    public DispatchResult dispatch(Event event) {
        long startTime = System.currentTimeMillis();

        DispatchResult result = origin.dispatch(event);

        long durationMs = System.currentTimeMillis() - startTime;
        logEvent(event, result, durationMs, null);

        return result;
    }

    private void logEvent(Event event, DispatchResult result, long durationMs, Throwable error) {
        String json = buildLogEntry(event, result, durationMs, error);

        if (error != null) {
            log.error(json);
        } else if (durationMs > 1000) {
            log.warn(json);
        } else {
            log.info(json);
        }
    }

    private String buildLogEntry(Event event, DispatchResult result, long durationMs, Throwable error) {
        Map<String, Object> entry = new LinkedHashMap<>();

        addIfPresent(entry, "traceId", MDC.get("traceId"));
        addIfPresent(entry, "spanId", MDC.get("spanId"));

        Map<String, Object> eventInfo = new LinkedHashMap<>();

        if (error != null) {
            eventInfo.put("status", "failed");
        } else {
            eventInfo.put("status", "handled");
        }

        buildEventId(eventInfo, event);
        buildPayload(eventInfo, event);

        if (result != null && !result.handlers().isEmpty()) {
            eventInfo.put("handlers", result.handlers().stream().map(h -> h.name()).toList());
            eventInfo.put("handlersCount", result.invokedHandlers());
        }
        eventInfo.put("durationMs", durationMs);

        if (error != null) {
            Map<String, Object> errorInfo = new LinkedHashMap<>();
            errorInfo.put("message", error.getMessage());
            errorInfo.put("type", error.getClass().getSimpleName());
            eventInfo.put("error", errorInfo);
        }

        entry.put("event", eventInfo);
        entry.put("@timestamp", Instant.now().toString());

        return toJson(entry);
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
                addPayloadFields(payloadInfo, json.substring(0, maxPayloadLength) + "...");
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