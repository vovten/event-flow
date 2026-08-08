package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.util.EventLogUtils;
import io.github.vovten.eventflow.util.JsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 * @since 1.1.0
 */
public final class LoggingEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventDispatcher.class);

    private final EventDispatcher origin;
    private final int maxPayloadLength;
    private final Set<String> excludedEvents;
    private final Map<String, String> logLevels;

    /**
     * Create logging decorator with default settings.
     *
     * @param origin the delegate dispatcher to wrap
     * @throws NullPointerException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin) {
        this(origin, 1024, Collections.emptySet(), Collections.emptyMap());
    }

    /**
     * Create logging decorator with custom max payload length.
     *
     * @param origin           the delegate dispatcher to wrap
     * @param maxPayloadLength maximum length of payload in log output
     * @throws NullPointerException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin, int maxPayloadLength) {
        this(origin, maxPayloadLength, Collections.emptySet(), Collections.emptyMap());
    }

    /**
     * Create logging decorator with custom max payload length and excluded event types.
     *
     * @param origin             the delegate dispatcher to wrap
     * @param maxPayloadLength   maximum length of payload in log output
     * @param excludedEvents set of event simple class names to exclude from logging
     * @throws NullPointerException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin, int maxPayloadLength, Set<String> excludedEvents) {
        this(origin, maxPayloadLength, excludedEvents, Collections.emptyMap());
    }

    /**
     * Create logging decorator with full configuration.
     *
     * @param origin             the delegate dispatcher to wrap
     * @param maxPayloadLength   maximum length of payload in log output
     * @param excludedEvents set of event simple class names to exclude from logging
     * @param logLevels     per-event minimum log level threshold (payload simple class name → level);
     *                      only ERROR and WARN suppress, other levels mean no suppression
     * @throws NullPointerException if origin is null
     */
    public LoggingEventDispatcher(EventDispatcher origin, int maxPayloadLength,
                                  Set<String> excludedEvents, Map<String, String> logLevels) {
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.maxPayloadLength = maxPayloadLength;
        this.excludedEvents = Objects.requireNonNullElseGet(excludedEvents, Collections::emptySet);
        this.logLevels = Objects.requireNonNullElseGet(logLevels, HashMap::new);
    }

    @Override
    public CompletableFuture<HandlerResults> dispatch(Event event) {
        if (isExcluded(event)) {
            return origin.dispatch(event);
        }
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

    private boolean isExcluded(Event event) {
        if (excludedEvents.isEmpty()) {
            return false;
        }
        Object payload = EventLogUtils.extractPayload(event);
        return excludedEvents.contains(payload.getClass().getSimpleName());
    }

    private void logEvent(Event event, HandlerResults results, Throwable error,
                          long durationMs, Instant start,
                          String traceId, String spanId, String deliveredFrom) {
        String entry = buildLogEntry(event, results, error, durationMs, start, traceId, spanId, deliveredFrom);
        String eventType = resolveEventType(event);
        String overrideLevel = logLevels.get(eventType);
        boolean isError = error != null || (results != null && results.isAllFailure());
        boolean isWarn = !isError && results != null && results.isPartialSuccess();

        if (overrideLevel != null && !isLoggable(isError, isWarn, overrideLevel)) {
            return;
        }
        if (isError) {
            log.error(entry);
        } else if (isWarn) {
            log.warn(entry);
        } else {
            log.info(entry);
        }
    }

    private String resolveEventType(Event event) {
        return EventLogUtils.extractPayload(event).getClass().getSimpleName();
    }

    private static boolean isLoggable(boolean isError, boolean isWarn, String minLevel) {
        int natural = isError ? 3 : isWarn ? 2 : 1;
        int min = switch (minLevel.toUpperCase()) {
            case "ERROR" -> 3;
            case "WARN"  -> 2;
            default      -> 1; // INFO, DEBUG, TRACE — log everything
        };
        return natural >= min;
    }

    private String buildLogEntry(Event event, HandlerResults results, Throwable error,
                                  long durationMs, Instant start,
                                  String traceId, String spanId, String deliveredFrom) {
        JsonBuilder jb = new JsonBuilder(256 + maxPayloadLength);
        jb.beginObject();
        jb.beginObject("event");
        appendStatus(jb, results, error);
        jb.appendString("eventId", EventLogUtils.extractEventId(event));
        jb.appendString("payload", EventLogUtils.extractPayloadString(event, maxPayloadLength));
        EventLogUtils.appendEnvelopeMetadata(jb, event);
        appendHandlerResults(jb, results);
        EventLogUtils.appendErrorInfo(jb, error);
        jb.appendNumber("durationMs", durationMs);
        jb.endObject();
        EventLogUtils.appendRootContext(jb, start, traceId, spanId, deliveredFrom);
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
