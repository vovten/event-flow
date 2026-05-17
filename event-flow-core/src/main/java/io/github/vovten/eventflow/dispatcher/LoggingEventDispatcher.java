package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
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
 * <p>
 * Optimized: StringBuilder for outer structure, cached reflection for payload fields.
 *
 * @author Vladimir Aleshkov
 * @see IdempotentEventDispatcher
 * @since 2026-05-11
 */
public class LoggingEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventDispatcher.class);

    // Cache for payload class reflection data
    private final EventDispatcher origin;

    private final int maxPayloadLength;

    /**
     * Create logging decorator with default settings.
     *
     * @param origin the delegate dispatcher to wrap
     * @throws IllegalArgumentException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin) {
        this(origin, 1024);
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
        String traceId = MDC.get("traceId");
        String spanId = MDC.get("spanId");

        return origin.dispatch(event)
                .whenComplete((results, error) -> {
                    long durationMs = System.currentTimeMillis() - startTime;
                    logEvent(event, results, error, durationMs, traceId, spanId);
                });
    }

    private void logEvent(Event event, HandlerResults results, Throwable error,
                          long durationMs, String traceId, String spanId) {
        String entry = buildLogEntry(event, results, error, durationMs, traceId, spanId);

        if (error != null || (results != null && results.isAllFailure())) {
            log.error(entry);
        } else if (results != null && results.isPartialSuccess()) {
            log.warn(entry);
        } else {
            log.info(entry);
        }
    }

    private String buildLogEntry(Event event, HandlerResults results, Throwable error,
                                 long durationMs, String traceId, String spanId) {
        StringBuilder sb = new StringBuilder(512);

        // tracing
        sb.append("{\"traceId\":");
        appendNullable(sb, traceId);
        sb.append(",\"spanId\":");
        appendNullable(sb, spanId);

        sb.append(",\"event\":{");

        // status
        sb.append("\"status\":\"");
        if (error != null) {
            sb.append("failed");
        } else if (results != null && !results.isEmpty()) {
            if (results.isAllSuccess()) {
                sb.append("handled");
            } else if (results.isPartialSuccess()) {
                sb.append("partial");
            } else {
                sb.append("failed");
            }
        } else {
            sb.append("handled");
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

        // handlers
        if (results != null && !results.isEmpty()) {
            List<HandlerResult> successes = results.getSuccesses();
            if (!successes.isEmpty()) {
                sb.append("\"handlers\":[");
                for (int i = 0; i < successes.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append("\"");
                    sb.append(successes.get(i).handlerName());
                    sb.append("\"");
                }
                sb.append("],");
            }

            if (results.getFailedCount() > 0) {
                sb.append("\"failedHandlers\":[");
                List<HandlerResult> failures = results.getFailures();
                for (int i = 0; i < failures.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append("\"");
                    sb.append(failures.get(i).handlerName());
                    String err = failures.get(i).errorDetails();
                    if (err != null) {
                        sb.append(": ");
                        sb.append(escape(err));
                    }
                    sb.append("\"");
                }
                sb.append("],");
                sb.append("\"failedCount\":");
                sb.append(results.getFailedCount());
                sb.append(",");
            }
        }

        // payload
        buildPayload(sb, event);

        // duration
        sb.append(",\"durationMs\":");
        sb.append(durationMs);

        // error
        if (error != null) {
            sb.append(",\"error\":{\"message\":\"");
            sb.append(escape(error.getMessage()));
            sb.append("\",\"type\":\"");
            sb.append(error.getClass().getSimpleName());
            sb.append("\"}");
        }

        sb.append("},\"@timestamp\":\"");
        sb.append(Instant.now());
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
        PayloadCache cache = PayloadCache.create(payloadClass);

        if (cache.fields.length > 0) {
            StringBuilder fieldsSb = new StringBuilder();
            appendPayloadFields(fieldsSb, payload, cache);
            String fieldsJson = fieldsSb.toString();

            if (fieldsJson.length() > maxPayloadLength) {
                sb.append("\"_truncated\":true,");
                sb.append("\"_originalSize\":");
                sb.append(fieldsJson.length());
                sb.append(",\"data\":\"");
                sb.append(escape(fieldsJson.substring(0, maxPayloadLength)));
                sb.append("...\"}");
            } else {
                sb.append(fieldsJson);
                sb.append("}");
            }
        } else {
            // Fallback: use EventUtils.toJson for types without getters
            try {
                String json = io.github.vovten.eventflow.util.EventUtils.toJson(payload);
                if (json.length() > maxPayloadLength) {
                    sb.append("\"_truncated\":true,");
                    sb.append("\"_originalSize\":");
                    sb.append(json.length());
                    sb.append(",\"data\":\"");
                    sb.append(escape(json.substring(0, maxPayloadLength)));
                    sb.append("...\"}");
                } else {
                    sb.append("\"data\":");
                    sb.append(json);
                    sb.append("}");
                }
            } catch (Exception e) {
                sb.append("\"_raw\":\"");
                sb.append(escape(payload.toString()));
                sb.append("\"}");
            }
        }
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

    private static final class PayloadCache {
        private static final Map<Class<?>, PayloadCache> CACHE = new ConcurrentHashMap<>();

        final String[] fields;
        final Method[] getters;

        private PayloadCache(Class<?> clazz) {
            var fieldNames = new java.util.ArrayList<String>();
            var getterMethods = new java.util.ArrayList<Method>();

            Class<?> current = clazz;
            while (current != null && current != Object.class) {
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
                    String getterName = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
                    Method getter = null;

                    try {
                        getter = clazz.getMethod("get" + getterName);
                    } catch (NoSuchMethodException e) {
                        if (field.getType() == boolean.class) {
                            try {
                                getter = clazz.getMethod("is" + getterName);
                            } catch (NoSuchMethodException ex) {
                                // ignored
                            }
                        }
                    }

                    if (getter != null) {
                        fieldNames.add(fieldName);
                        getterMethods.add(getter);
                    }
                }
                current = current.getSuperclass();
            }

            this.fields = fieldNames.toArray(new String[0]);
            this.getters = getterMethods.toArray(new Method[0]);
        }

        private PayloadCache(String[] fields, Method[] getters) {
            this.fields = fields;
            this.getters = getters;
        }

        static PayloadCache create(Class<?> clazz) {
            return CACHE.computeIfAbsent(clazz, PayloadCache::new);
        }
    }
}