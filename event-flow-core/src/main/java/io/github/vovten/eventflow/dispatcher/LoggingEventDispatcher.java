package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Decorator for {@link EventDispatcher} that adds structured logging when events are dispatched.
 * <p>
 * Logs event handling information with machine-parseable JSON format:
 * <ul>
 *   <li>status - handling result (handled/partial/failed)</li>
 *   <li>eventId - envelope identifier</li>
 *   <li>payload - event payload as string (truncated if too long)</li>
 *   <li>occurredAt - event occurrence timestamp</li>
 *   <li>processId - process correlation identifier</li>
 *   <li>handlers - list of successful handler names</li>
 *   <li>failedHandlers - list of failed handler names with errors</li>
 *   <li>failedCount - number of failed handlers</li>
 *   <li>durationMs - processing duration in milliseconds</li>
 *   <li>error - error details if dispatch failed</li>
 *   <li>traceId - distributed trace ID from MDC</li>
 *   <li>spanId - span ID from MDC</li>
 *   <li>deliveredFrom - source identifier from MDC</li>
 * </ul>
 * <p>
 * Logging is performed after the dispatch operation completes.
 * MDC context is read at log time for tracing fields.
 * <p>
 *
 * @author Vladimir Aleshkov
 * @see IdempotentEventDispatcher
 * @since 2026-05-11
 */
public class LoggingEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventDispatcher.class);

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
     * @param origin           the delegate dispatcher to wrap
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
        return origin.dispatch(event)
                .whenComplete((results, error) -> {
                    long durationMs = System.currentTimeMillis() - startTime;
                    logEvent(event, results, error, durationMs);
                });
    }

    private void logEvent(Event event, HandlerResults results, Throwable error, long durationMs) {
        String entry = buildLogEntry(event, results, error, durationMs);

        if (error != null || (results != null && results.isAllFailure())) {
            log.error(entry);
        } else if (results != null && results.isPartialSuccess()) {
            log.warn(entry);
        } else {
            log.info(entry);
        }
    }

    private String buildLogEntry(Event event, HandlerResults results, Throwable error, long durationMs) {
        StringBuilder sb = new StringBuilder(256 + maxPayloadLength);
        sb.append("{\"event\":{");
        appendStatus(sb, results, error);
        appendEventId(sb, event);
        appendPayload(sb, event);
        appendEnvelopeMetadata(sb, event);
        appendHandlerResults(sb, results);
        appendErrorInfo(sb, error);
        appendDuration(sb, durationMs);
        sb.append("}");
        appendRootContext(sb);
        return sb.toString();
    }

    private void appendStatus(StringBuilder sb, HandlerResults results, Throwable error) {
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
            sb.append("skipped");
        }
        sb.append("\",");
        if (error != null) {
            sb.append("\"statusDesc\":\"");
            sb.append(escape(error.getMessage()));
            sb.append("\",");
        } else if (results != null && results.getSkipReason() != null) {
            sb.append("\"statusDesc\":\"");
            sb.append(results.getSkipReason());
            sb.append("\",");
        }
    }

    private void appendEventId(StringBuilder sb, Event event) {
        sb.append("\"eventId\":\"");
        if (event instanceof TraceableEvent te && te.eventId() != null) {
            sb.append(te.eventId());
        } else {
            sb.append("unknown");
        }
        sb.append("\",");
    }

    private void appendPayload(StringBuilder sb, Event event) {
        sb.append("\"payload\":\"");
        Object payloadObj = extractPayload(event);
        if (payloadObj != null) {
            String payloadStr = payloadObj.toString();
            if (payloadStr.length() > maxPayloadLength) {
                payloadStr = payloadStr.substring(0, maxPayloadLength) + "...";
            }
            sb.append(escape(payloadStr));
        }
        sb.append("\",");
    }

    private void appendEnvelopeMetadata(StringBuilder sb, Event event) {
        if (event instanceof TraceableEvent te) {
            if (te.processId() != null) {
                sb.append("\"processId\":\"");
                sb.append(te.processId());
                sb.append("\",");
            }
            if (te.occurredAt() != null) {
                sb.append("\"occurredAt\":\"");
                sb.append(te.occurredAt());
                sb.append("\",");
            }
        }
    }

    private void appendHandlerResults(StringBuilder sb, HandlerResults results) {
        if (results == null || results.isEmpty()) {
            return;
        }
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

    private void appendErrorInfo(StringBuilder sb, Throwable error) {
        if (error == null) {
            return;
        }
        sb.append("\"error\":{\"message\":\"");
        sb.append(escape(error.getMessage()));
        sb.append("\",\"type\":\"");
        sb.append(error.getClass().getSimpleName());
        sb.append("\"},");
    }

    private void appendDuration(StringBuilder sb, long durationMs) {
        sb.append("\"durationMs\":");
        sb.append(durationMs);
    }

    private void appendRootContext(StringBuilder sb) {
        String traceId = MDC.get("traceId");
        if (traceId != null) {
            sb.append(",\"traceId\":\"");
            sb.append(traceId);
            sb.append("\"");
        }
        String spanId = MDC.get("spanId");
        if (spanId != null) {
            sb.append(",\"spanId\":\"");
            sb.append(spanId);
            sb.append("\"");
        }
        String deliveredFrom = MDC.get("deliveredFrom");
        if (deliveredFrom != null) {
            sb.append(",\"deliveredFrom\":\"");
            sb.append(deliveredFrom);
            sb.append("\"");
        }
        sb.append(",\"@timestamp\":\"");
        sb.append(Instant.now());
        sb.append("\"}");
    }

    private Object extractPayload(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload();
        }
        return event;
    }

    private String escape(String value) {
        if (value == null) {
            return "";
        }
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
}
