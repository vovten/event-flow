package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
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
 * MDC context is read at log time for tracing fields.
 * <p>
 *
 * @author Vladimir Aleshkov
 * @see ChannelEventPublisher
 * @see RetryEventPublisher
 * @since 2026-05-11
 */
public class LoggingEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventPublisher.class);

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
     * @param origin           the delegate publisher to wrap
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
        return origin.publish(event)
                .whenComplete((result, error) ->
                        logEvent(event, result, error, start));
    }

    private void logEvent(Event event, SendResults result, Throwable error,
                          Instant start) {
        String entry = buildLogEntry(event, result, error, start);

        if (error != null || (result != null && result.isAllFailure())) {
            log.error(entry);
        } else if (result != null && result.isPartialSuccess()) {
            log.warn(entry);
        } else {
            log.info(entry);
        }
    }

    private String buildLogEntry(Event event, SendResults result, Throwable error,
                                 Instant start) {
        StringBuilder sb = new StringBuilder(256 + maxPayloadLength);
        sb.append("{\"event\":{");
        appendStatus(sb, result, error);
        appendEventId(sb, event);
        appendPayload(sb, event);
        appendEnvelopeMetadata(sb, event);
        appendChannels(sb, event);
        appendDeliveryResults(sb, result);
        appendErrorInfo(sb, error);
        sb.append("}");
        appendRootContext(sb, start);
        return sb.toString();
    }

    private void appendStatus(StringBuilder sb, SendResults result, Throwable error) {
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

    private void appendChannels(StringBuilder sb, Event event) {
        sb.append("\"channels\":[");
        var channels = event.channels();
        if (!channels.isEmpty()) {
            for (int i = 0; i < channels.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("\"");
                sb.append(channels.get(i).getSimpleName());
                sb.append("\"");
            }
        }
        sb.append("]");
    }

    private void appendDeliveryResults(StringBuilder sb, SendResults result) {
        if (result == null || result.isEmpty()) {
            return;
        }
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
                if (err != null) {
                    sb.append(escape(err));
                }
                sb.append("\"");
            }
            sb.append("]");
        }
    }

    private void appendErrorInfo(StringBuilder sb, Throwable error) {
        if (error == null) {
            return;
        }
        sb.append(",\"error\":{\"message\":\"");
        sb.append(escape(error.getMessage()));
        sb.append("\",\"type\":\"");
        sb.append(error.getClass().getSimpleName());
        sb.append("\"}");
    }

    private void appendRootContext(StringBuilder sb, Instant start) {
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
        sb.append(start);
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
}