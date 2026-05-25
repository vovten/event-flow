package io.github.vovten.eventflow.util;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;

import java.time.Instant;

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

    private EventLogUtils() {
        // utility class
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
