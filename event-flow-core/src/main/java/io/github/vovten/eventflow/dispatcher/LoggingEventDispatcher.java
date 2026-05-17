package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.util.JsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Decorator for {@link EventDispatcher} that adds structured logging when events are dispatched.
 * <p>
 * Logs event handling information with machine-parseable JSON format:
 * <ul>
 *   <li>status - handling result (handled/partial/failed/skipped)</li>
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
 * MDC context is captured before the dispatch to ensure it reflects the caller's context.
 *
 * @author Vladimir Aleshkov
 * @see IdempotentEventDispatcher
 * @since 2026-05-11
 */
public final class LoggingEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventDispatcher.class);

    private final EventDispatcher origin;
    private final int maxPayloadLength;

    /**
     * Create logging decorator with default settings.
     *
     * @param origin the delegate dispatcher to wrap
     * @throws NullPointerException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin) {
        this(origin, 1024);
    }

    /**
     * Create logging decorator with custom max payload length.
     *
     * @param origin           the delegate dispatcher to wrap
     * @param maxPayloadLength maximum length of payload in log output
     * @throws NullPointerException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin, int maxPayloadLength) {
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.maxPayloadLength = maxPayloadLength;
    }

    @Override
    public CompletableFuture<HandlerResults> dispatch(Event event) {
        Instant start = Instant.now();
        String traceId = MDC.get("traceId");
        String spanId = MDC.get("spanId");
        String deliveredFrom = MDC.get("deliveredFrom");
        return origin.dispatch(event)
                .whenComplete((results, error) -> {
                    long durationMs = Duration.between(start, Instant.now()).toMillis();
                    logEvent(event, results, error, durationMs, start, traceId, spanId, deliveredFrom);
                });
    }

    private void logEvent(Event event, HandlerResults results, Throwable error,
                          long durationMs, Instant start,
                          String traceId, String spanId, String deliveredFrom) {
        String entry = buildLogEntry(event, results, error, durationMs, start, traceId, spanId, deliveredFrom);

        if (error != null || (results != null && results.isAllFailure())) {
            log.error(entry);
        } else if (results != null && results.isPartialSuccess()) {
            log.warn(entry);
        } else {
            log.info(entry);
        }
    }

    private String buildLogEntry(Event event, HandlerResults results, Throwable error,
                                  long durationMs, Instant start,
                                  String traceId, String spanId, String deliveredFrom) {
        JsonBuilder jb = new JsonBuilder(256 + maxPayloadLength);
        jb.beginObject();
        jb.beginObject("event");
        appendStatus(jb, results, error);
        jb.appendString("eventId", extractEventId(event));
        jb.appendString("payload", extractPayloadString(event));
        appendEnvelopeMetadata(jb, event);
        appendHandlerResults(jb, results);
        appendErrorInfo(jb, error);
        jb.appendNumber("durationMs", durationMs);
        jb.endObject();
        appendRootContext(jb, start, traceId, spanId, deliveredFrom);
        jb.endObject();
        return jb.build();
    }

    private void appendStatus(JsonBuilder jb, HandlerResults results, Throwable error) {
        jb.appendString("status", computeStatus(results, error));
        appendStatusDesc(jb, results);
    }

    private static String computeStatus(HandlerResults results, Throwable error) {
        if (error != null) {
            return "failed";
        }
        if (results != null && !results.isEmpty()) {
            if (results.isAllSuccess()) {
                return "handled";
            }
            if (results.isPartialSuccess()) {
                return "partial";
            }
            return "failed";
        }
        return "skipped";
    }

    private static void appendStatusDesc(JsonBuilder jb, HandlerResults results) {
        if (results == null) {
            return;
        }
        if (!results.isEmpty() && results.getFailedCount() > 0) {
            jb.appendString("statusDesc", "some handlers failed");
        } else if (results.getSkipReason() != null) {
            jb.appendString("statusDesc", results.getSkipReason());
        }
    }

    private void appendEnvelopeMetadata(JsonBuilder jb, Event event) {
        if (event instanceof TraceableEvent te) {
            if (te.processId() != null) {
                jb.appendString("processId", te.processId().toString());
            }
            if (te.occurredAt() != null) {
                jb.appendString("occurredAt", te.occurredAt().toString());
            }
        }
    }

    private void appendHandlerResults(JsonBuilder jb, HandlerResults results) {
        if (results == null || results.isEmpty()) {
            return;
        }
        if (results.getSuccessfulCount() > 0) {
            jb.beginArray("handlers");
            for (HandlerResult r : results.getSuccesses()) {
                jb.appendArrayItem(r.handlerName());
            }
            jb.endArray();
        }
        if (results.getFailedCount() > 0) {
            jb.beginArray("failedHandlers");
            for (HandlerResult r : results.getFailures()) {
                jb.appendArrayItem(formatFailedHandler(r));
            }
            jb.endArray();
            jb.appendNumber("failedCount", results.getFailedCount());
        }
    }

    private static String formatFailedHandler(HandlerResult r) {
        String err = r.errorDetails();
        if (err != null) {
            return r.handlerName() + ": " + err;
        }
        return r.handlerName();
    }

    private void appendErrorInfo(JsonBuilder jb, Throwable error) {
        if (error == null) {
            return;
        }
        jb.beginObject("error");
        jb.appendString("message", error.getMessage());
        jb.appendString("type", error.getClass().getSimpleName());
        jb.endObject();
    }

    private void appendRootContext(JsonBuilder jb, Instant start,
                                    String traceId, String spanId, String deliveredFrom) {
        if (traceId != null) {
            jb.appendString("traceId", traceId);
        }
        if (spanId != null) {
            jb.appendString("spanId", spanId);
        }
        if (deliveredFrom != null) {
            jb.appendString("deliveredFrom", deliveredFrom);
        }
        jb.appendString("@timestamp", start.toString());
    }

    private String extractPayloadString(Event event) {
        Object payloadObj = extractPayload(event);
        if (payloadObj == null) {
            return "";
        }
        String payloadStr = payloadObj.toString();
        if (payloadStr.length() > maxPayloadLength) {
            return payloadStr.substring(0, maxPayloadLength) + "...";
        }
        return payloadStr;
    }

    private static String extractEventId(Event event) {
        if (event instanceof TraceableEvent te && te.eventId() != null) {
            return te.eventId().toString();
        }
        return "unknown";
    }

    private static Object extractPayload(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload();
        }
        return event;
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
