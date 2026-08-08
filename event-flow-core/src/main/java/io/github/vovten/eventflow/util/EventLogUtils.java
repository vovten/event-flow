package io.github.vovten.eventflow.util;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Shared utility methods for structured event logging.
 * <p>
 * Contains methods used by both {@code LoggingEventPublisher} and
 * {@code LoggingEventDispatcher} to avoid code duplication when building
 * JSON-structured log entries for event operations.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public final class EventLogUtils {

    private static final Logger log = LoggerFactory.getLogger(EventLogUtils.class);

    /**
     * A valid Java simple class name, as returned by {@code Class.getSimpleName()}.
     */
    private static final Pattern SIMPLE_CLASS_NAME = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*");

    /**
     * Accepted log level names. {@code TRACE}/{@code DEBUG} mean no suppression,
     * only {@code ERROR} and {@code WARN} narrow the output.
     */
    private static final Set<String> VALID_LOG_LEVELS = Set.of("TRACE", "DEBUG", "INFO", "WARN", "ERROR");

    private EventLogUtils() {
        // utility class
    }

    /**
     * Validate a {@code log-levels} configuration map at startup.
     * <p>
     * Keys must be valid simple class names (no package): a fully-qualified name,
     * empty value, or malformed identifier can never match an event type and is
     * reported with a {@code WARN} so misconfiguration is not silent.
     * Values must be a known log level ({@code TRACE}, {@code DEBUG}, {@code INFO},
     * {@code WARN}, {@code ERROR}).
     * <p>
     * The map itself is not modified; the decorator still behaves as before for
     * entries that could not be validated.
     *
     * @param logLevels the configured map (may be {@code null} or empty)
     * @param context   human-readable description of the config source for log messages
     */
    public static void validateLogLevelConfig(Map<String, String> logLevels, String context) {
        if (logLevels == null || logLevels.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : logLevels.entrySet()) {
            String key = entry.getKey();
            if (key == null || !SIMPLE_CLASS_NAME.matcher(key).matches()) {
                log.warn("Invalid log-levels key '{}' in {}: expected a simple class name without a package "
                        + "(e.g. \"HeartbeatEvent\"). This entry will never match an event type.", key, context);
            }
            String level = entry.getValue();
            if (level == null || !VALID_LOG_LEVELS.contains(level.toUpperCase(Locale.ROOT))) {
                log.warn("Invalid log-levels value '{}' for key '{}' in {}: expected one of {}. "
                        + "This entry is ignored.", level, key, context, VALID_LOG_LEVELS);
            }
        }
    }

    /**
     * Append event metadata (processId, occurredAt) to the JSON builder.
     *
     * @param jb    the JSON builder
     * @param event the event to extract metadata from
     */
    public static void appendEnvelopeMetadata(JsonBuilder jb, Event event) {
        if (event instanceof TraceableEvent te) {
            if (te.processId() != null) {
                jb.appendString("processId", te.processId().toString());
            }
            if (te.occurredAt() != null) {
                jb.appendString("occurredAt", te.occurredAt().toString());
            }
        }
    }

    /**
     * Append error information (message, type) to the JSON builder.
     *
     * @param jb    the JSON builder
     * @param error the error to append (may be null)
     */
    public static void appendErrorInfo(JsonBuilder jb, Throwable error) {
        if (error == null) {
            return;
        }
        jb.beginObject("error");
        jb.appendString("message", error.getMessage());
        jb.appendString("type", error.getClass().getSimpleName());
        jb.endObject();
    }

    /**
     * Append root-level context fields (traceId, spanId, deliveredFrom, @timestamp)
     * to the JSON builder.
     *
     * @param jb            the JSON builder
     * @param start         the start timestamp for @timestamp field
     * @param traceId       trace ID from MDC (may be null)
     * @param spanId        span ID from MDC (may be null)
     * @param deliveredFrom source identifier from MDC (may be null)
     */
    public static void appendRootContext(JsonBuilder jb, Instant start,
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

    /**
     * Extract a human-readable event identifier from an event.
     *
     * @param event the event
     * @return the event ID as string, or {@code "unknown"} if not traceable
     */
    public static String extractEventId(Event event) {
        if (event instanceof TraceableEvent te && te.eventId() != null) {
            return te.eventId().toString();
        }
        return "unknown";
    }

    /**
     * Extract the payload object from an event.
     * <p>
     * If the event is an {@link Envelope}, returns the wrapped payload.
     * Otherwise, returns the event itself.
     *
     * @param event the event
     * @return the payload object
     */
    public static Object extractPayload(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload();
        }
        return event;
    }

    /**
     * Extract the payload as a string, truncated to the specified maximum length.
     *
     * @param event            the event
     * @param maxPayloadLength maximum length of the payload string
     * @return the payload string, possibly truncated
     */
    public static String extractPayloadString(Event event, int maxPayloadLength) {
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
}
