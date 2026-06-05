package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.util.EventLogUtils;
import io.github.vovten.eventflow.util.JsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
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
 *   <li>payload - event payload as string (truncated if too long)</li>
 *   <li>status - publication result (published/partial/failed)</li>
 *   <li>deliveredTo - list of successfully delivered destinations</li>
 *   <li>failedOn - list of failed destinations (if partial/failure)</li>
 *   <li>traceId - distributed trace ID from MDC</li>
 *   <li>spanId - span ID from MDC</li>
 *   <li>deliveredFrom - source identifier from MDC</li>
 * </ul>
 * <p>
 * Logging is performed asynchronously after the publish operation completes,
 * allowing capture of the actual result status and channel delivery details.
 * MDC context is captured before the publish to ensure it reflects the caller's context.
 *
 * @author Vladimir Aleshkov
 * @see ChannelEventPublisher
 * @see RetryEventPublisher
 * @since 1.1.0
 */
public final class LoggingEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventPublisher.class);

    private final EventPublisher origin;
    private final int maxPayloadLength;
    private final Set<String> excludedEvents;

    /**
     * Create logging decorator with default settings.
     *
     * @param origin the delegate publisher to wrap
     * @throws NullPointerException if origin is null
     */
    public LoggingEventPublisher(EventPublisher origin) {
        this(origin, 1024, Collections.emptySet());
    }

    /**
     * Create logging decorator with custom max payload length.
     *
     * @param origin           the delegate publisher to wrap
     * @param maxPayloadLength maximum length of payload in log output
     * @throws NullPointerException if origin is null
     */
    public LoggingEventPublisher(EventPublisher origin, int maxPayloadLength) {
        this(origin, maxPayloadLength, Collections.emptySet());
    }

    /**
     * Create logging decorator with custom max payload length and excluded event types.
     *
     * @param origin             the delegate publisher to wrap
     * @param maxPayloadLength   maximum length of payload in log output
     * @param excludedEvents set of event simple class names to exclude from logging
     * @throws NullPointerException if origin is null
     */
    public LoggingEventPublisher(EventPublisher origin, int maxPayloadLength, Set<String> excludedEvents) {
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.maxPayloadLength = maxPayloadLength;
        this.excludedEvents = Objects.requireNonNullElseGet(excludedEvents, Collections::emptySet);
    }

    @Override
    public CompletableFuture<SendResults> publish(Event event) {
        if (isExcluded(event)) {
            return origin.publish(event);
        }
        Instant start = Instant.now();
        String traceId = MDC.get("traceId");
        String spanId = MDC.get("spanId");
        String deliveredFrom = MDC.get("deliveredFrom");
        return origin.publish(event)
                .whenComplete((result, error) ->
                        logEvent(event, result, error, start, traceId, spanId, deliveredFrom));
    }

    private boolean isExcluded(Event event) {
        if (excludedEvents.isEmpty()) {
            return false;
        }
        Object payload = EventLogUtils.extractPayload(event);
        return excludedEvents.contains(payload.getClass().getSimpleName());
    }

    private void logEvent(Event event, SendResults result, Throwable error,
                          Instant start, String traceId, String spanId, String deliveredFrom) {
        String entry = buildLogEntry(event, result, error, start, traceId, spanId, deliveredFrom);

        if (error != null || (result != null && result.isAllFailure())) {
            log.error(entry);
        } else if (result != null && result.isPartialSuccess()) {
            log.warn(entry);
        } else {
            log.info(entry);
        }
    }

    private String buildLogEntry(Event event, SendResults result, Throwable error,
                                  Instant start, String traceId, String spanId, String deliveredFrom) {
        JsonBuilder jb = new JsonBuilder(256 + maxPayloadLength);
        jb.beginObject();
        jb.beginObject("event");
        appendStatus(jb, result, error);
        jb.appendString("eventId", EventLogUtils.extractEventId(event));
        jb.appendString("payload", EventLogUtils.extractPayloadString(event, maxPayloadLength));
        EventLogUtils.appendEnvelopeMetadata(jb, event);
        appendChannels(jb, event);
        appendDeliveryResults(jb, result);
        EventLogUtils.appendErrorInfo(jb, error);
        jb.endObject();
        EventLogUtils.appendRootContext(jb, start, traceId, spanId, deliveredFrom);
        jb.endObject();
        return jb.build();
    }

    private void appendStatus(JsonBuilder jb, SendResults result, Throwable error) {
        jb.appendString("status", computeStatus(result, error));
    }

    private static String computeStatus(SendResults result, Throwable error) {
        if (error != null) {
            return "failed";
        }
        if (result != null) {
            if (result.isAllSuccess()) {
                return "published";
            }
            if (result.isPartialSuccess()) {
                return "partial";
            }
            if (result.isAllFailure()) {
                return "failed";
            }
        }
        return "unknown";
    }

    private void appendChannels(JsonBuilder jb, Event event) {
        var channels = event.channels();
        jb.beginArray("channels");
        for (var channel : channels) {
            jb.appendArrayItem(channel.getSimpleName());
        }
        jb.endArray();
    }

    private void appendDeliveryResults(JsonBuilder jb, SendResults result) {
        if (result == null || result.isEmpty()) {
            return;
        }
        jb.beginArray("deliveredTo");
        for (SendResult s : result.getSuccesses()) {
            jb.appendArrayItem(s.destination());
        }
        jb.endArray();

        if (result.isPartialSuccess() || result.isAllFailure()) {
            jb.beginArray("failedOn");
            for (SendResult f : result.getFailures()) {
                jb.appendArrayItem(formatFailedDestination(f));
            }
            jb.endArray();
        }
    }

    private static String formatFailedDestination(SendResult f) {
        String err = f.errorDetails();
        if (err != null) {
            return f.destination() + ": " + err;
        }
        return f.destination();
    }
}