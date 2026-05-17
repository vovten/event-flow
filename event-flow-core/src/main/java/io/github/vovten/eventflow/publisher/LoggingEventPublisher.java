package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

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
 * <p>
 * Optimized implementation: uses StringBuilder for outer structure, and
 * cached reflection for payload field extraction (reflection happens once per payload type).
 *
 * @author Vladimir Aleshkov
 * @see ChannelEventPublisher
 * @see RetryEventPublisher
 * @since 2026-05-11
 */
public class LoggingEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventPublisher.class);
    private static final int MAX_PAYLOAD_LENGTH = 1024;

    private final EventPublisher origin;
    private final int maxPayloadLength;

    /**
     * Create logging decorator with default settings.
     *
     * @param origin the delegate publisher to wrap
     * @throws IllegalArgumentException if origin is null
     */
    public LoggingEventPublisher(EventPublisher origin) {
        this(origin, 1024);
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
        String traceId = MDC.get("traceId");
        String spanId = MDC.get("spanId");
        return origin.publish(event)
                .whenComplete((result, error) -> logEvent(event, result, error, start, traceId, spanId));
    }

    private void logEvent(Event event, SendResults result, Throwable error,
                          Instant start, String traceId, String spanId) {
        String entry = buildLogEntry(event, result, error, start, traceId, spanId);

        if (error != null || (result != null && result.isAllFailure())) {
            log.error(entry);
        } else if (result != null && result.isPartialSuccess()) {
            log.warn(entry);
        } else {
            log.info(entry);
        }
    }

    private String buildLogEntry(Event event, SendResults result, Throwable error,
                                 Instant start, String traceId, String spanId) {
        StringBuilder sb = new StringBuilder(512);

        // traceId
        sb.append("{\"traceId\":");
        appendNullable(sb, traceId);
        sb.append(",\"spanId\":");
        appendNullable(sb, spanId);

        sb.append(",\"event\":{");

        // status
        sb.append("\"status\":\"");
        if (error != null) {
            sb.append("failed");
        } else if (result != null && result.isAllSuccess()) {
            sb.append("published");
        } else if (result != null && result.isPartialSuccess()) {
            sb.append("partial");
        } else if (result != null && result.isAllFailure()) {
            sb.append("failed");
        } else {
            sb.append("unknown");
        }
        sb.append("\",");

        // eventId
        sb.append("\"eventId\":\"");
        if (event instanceof TraceableEvent te && te.eventId() != null) {
            sb.append(te.eventId());
        } else {
            sb.append("unknown");
        }
        sb.append("\",");

        // processId
        if (event instanceof TraceableEvent te && te.processId() != null) {
            sb.append("\"processId\":\"");
            sb.append(te.processId());
            sb.append("\",");
        }

        // occurredAt
        if (event instanceof TraceableEvent te && te.occurredAt() != null) {
            sb.append("\"occurredAt\":\"");
            sb.append(te.occurredAt());
            sb.append("\",");
        }

        // channels
        sb.append("\"channels\":[");
        var channels = event.channels();
        if (!channels.isEmpty()) {
            for (int i = 0; i < channels.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("\"");
                sb.append(channels.get(i).getClass().getSimpleName());
                sb.append("\"");
            }
        }
        sb.append("],");

        // payload
        buildPayload(sb, event);

        // deliveredTo
        if (result != null && !result.isEmpty()) {
            sb.append(",\"deliveredTo\":[");
            List<SendResult> successes = result.getSuccesses();
            for (int i = 0; i < successes.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("\"");
                sb.append(successes.get(i).destination());
                sb.append("\"");
            }
            sb.append("]");

            if (result.isPartialSuccess() || result.isAllFailure()) {
                sb.append(",\"failedOn\":[");
                List<SendResult> failures = result.getFailures();
                for (int i = 0; i < failures.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append("\"");
                    sb.append(failures.get(i).destination());
                    sb.append(": ");
                    String err = failures.get(i).errorDetails();
                    if (err != null) sb.append(escape(err));
                    sb.append("\"");
                }
                sb.append("]");
            }
        }

        // error
        if (error != null) {
            sb.append(",\"error\":{\"message\":\"");
            sb.append(escape(error.getMessage()));
            sb.append("\",\"type\":\"");
            sb.append(error.getClass().getSimpleName());
            sb.append("\"}");
        }

        sb.append("},");
        sb.append("\"@timestamp\":\"");
        sb.append(start);
        sb.append("\"}");

        return sb.toString();
    }

    private void buildPayload(StringBuilder sb, Event event) {
        Object payload = extractPayload(event);
        sb.append("\"payload\":{");

        if (payload == null) {
            sb.append("\"@class\":\"null\"}");
            return;
        }

        sb.append("\"@class\":\"");
        sb.append(payload.getClass().getName());
        sb.append("\",");

        // Use cached reflection for payload fields
        Class<?> payloadClass = payload.getClass();
        PayloadCache cache = PAYLOAD_CACHE.computeIfAbsent(payloadClass, PayloadCache::create);

        if (cache.fields.length > 0) {
            appendPayloadFields(sb, payload, cache);
        } else {
            sb.append("\"_raw\":\"");
            sb.append(escape(payload.toString()));
            sb.append("\"}");
            return;
        }

        sb.append("}");
    }

    private void appendPayloadFields(StringBuilder sb, Object payload, PayloadCache cache) {
        for (int i = 0; i < cache.fields.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"");
            sb.append(cache.fields[i]);
            sb.append("\":");

            try {
                Object value = cache.getters[i].invoke(payload);
                appendValue(sb, value);
            } catch (Exception e) {
                sb.append("null");
            }
        }
    }

    private void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            sb.append("\"");
            sb.append(escape(s));
            sb.append("\"");
        } else if (value instanceof Number n) {
            sb.append(n.toString());
        } else if (value instanceof Boolean b) {
            sb.append(b.toString());
        } else if (value instanceof java.util.UUID uuid) {
            sb.append("\"");
            sb.append(uuid.toString());
            sb.append("\"");
        } else if (value instanceof Instant i) {
            sb.append("\"");
            sb.append(i.toString());
            sb.append("\"");
        } else {
            sb.append("\"");
            sb.append(escape(value.toString()));
            sb.append("\"");
        }
    }

    private Object extractPayload(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload();
        }
        return event;
    }

    private void appendNullable(StringBuilder sb, String value) {
        if (value != null) {
            sb.append("\"");
            sb.append(value);
            sb.append("\"");
        } else {
            sb.append("null");
        }
    }

    private String escape(String value) {
        if (value == null) return "";
        StringBuilder sb = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    // Cache for payload class reflection data
    private static final Map<Class<?>, PayloadCache> PAYLOAD_CACHE = new ConcurrentHashMap<>();

    private static class PayloadCache {
        final String[] fields;
        final Method[] getters;

        private PayloadCache(String[] fields, Method[] getters) {
            this.fields = fields;
            this.getters = getters;
        }

        static PayloadCache create(Class<?> clazz) {
            // Collect getters for fields
            var fieldNames = new java.util.ArrayList<String>();
            var getterMethods = new java.util.ArrayList<Method>();

            // Get fields from class and superclasses (excluding Event/TraceableEvent base classes)
            Class<?> current = clazz;
            while (current != null && current != Object.class) {
                // Skip event infrastructure classes
                if (current.getName().startsWith("io.github.vovten.eventflow.event")) {
                    current = current.getSuperclass();
                    continue;
                }

                for (java.lang.reflect.Field field : current.getDeclaredFields()) {
                    if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || 
                        java.lang.reflect.Modifier.isTransient(field.getModifiers())) {
                        continue;
                    }

                    String fieldName = field.getName();
                    
                    // Try getter method (getXxx or isXxx for boolean)
                    String getterName = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
                    Method getter = null;
                    
                    try {
                        getter = clazz.getMethod("get" + getterName);
                    } catch (NoSuchMethodException e) {
                        if (field.getType() == boolean.class) {
                            try {
                                getter = clazz.getMethod("is" + getterName);
                            } catch (NoSuchMethodException ignored) {}
                        }
                    }

                    if (getter != null) {
                        fieldNames.add(fieldName);
                        getterMethods.add(getter);
                    }
                }
                current = current.getSuperclass();
            }

            return new PayloadCache(
                fieldNames.toArray(new String[0]),
                getterMethods.toArray(new Method[0])
            );
        }
    }
}