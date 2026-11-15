package io.github.vovten.eventflow.transport.incoming.kafka;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializationException;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.AsyncEventConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Deserializes Kafka records and delivers events to the asynchronous consumer.
 * <p>
 * Failure classification:
 * <ul>
 *   <li><b>Tombstone</b> ({@code value == null}) — not an event, skipped and its
 *       offset may advance.</li>
 *   <li><b>Poison pill</b> ({@link EventSerializationException}) — the record can
 *       never be deserialized, skipped so the offset frontier advances past it.</li>
 *   <li><b>Any other error</b> (for example a serializer configuration problem) —
 *       converted into an exceptional future so the partition is blocked and the
 *       record is re-delivered after a restart instead of being silently lost.</li>
 * </ul>
 * <p>
 * The delivery diagnostics key {@code deliveredFrom} is propagated into the
 * asynchronous completion callback and the previous MDC value is restored.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class KafkaRecordDelivery {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordDelivery.class);

    private static final String MDC_DELIVERED_FROM = "deliveredFrom";

    private final EventSerializerFactory serializerFactory;
    private final String transportName;

    /**
     * Creates the record delivery service.
     *
     * @param serializerFactory serializer factory for event deserialization
     * @param transportName     transport name used in the delivery diagnostics
     */
    public KafkaRecordDelivery(EventSerializerFactory serializerFactory, String transportName) {
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory must not be null");
        this.transportName = Objects.requireNonNull(transportName, "transportName must not be null");
    }

    /**
     * Deserializes and delivers a single record.
     *
     * @param record        the Kafka record
     * @param eventConsumer the event consumer
     * @return the processing future to track for offset commitment; poison pills
     *         and tombstones return an already completed future so that the
     *         offset frontier advances past them (skip)
     */
    public CompletableFuture<?> deliver(ConsumerRecord<String, byte[]> record,
                                        AsyncEventConsumer eventConsumer) {
        byte[] data = record.value();
        if (data == null) {
            // Kafka tombstone: not an event. Skip and advance the frontier,
            // otherwise it would be re-delivered forever.
            log.warn("Skipping tombstone record from topic: {}, partition: {}, offset: {} — "
                            + "its offset will be committed",
                    record.topic(), record.partition(), record.offset());
            return CompletableFuture.completedFuture(null);
        }
        try {
            EventSerializer serializer = serializerFactory.getByData(data);
            Event event = serializer.deserialize(data, Event.class);
            if (event == null) {
                throw new EventSerializationException("Deserializer returned null for event data");
            }
            return dispatch(record, event, eventConsumer);
        } catch (EventSerializationException e) {
            // Poison pill: the record can never be deserialized, skipping its
            // offset avoids an endless redelivery loop. A completed future
            // lets the contiguous frontier advance past the poison record.
            // A dead-letter policy is a planned follow-up.
            log.error("Failed to deserialize event from topic: {}, partition: {}, offset: {}, key: {} — "
                            + "record is skipped and its offset will be committed",
                    record.topic(), record.partition(), record.offset(), record.key(), e);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            // Not a data-format problem (e.g. serializer configuration error):
            // do not silently drop the record — fail it so the partition is
            // blocked and the record is re-delivered after restart.
            log.error("Unexpected error while preparing event from topic: {}, partition: {}, offset: {}, "
                            + "key: {} — the partition will be blocked and the record re-delivered after restart",
                    record.topic(), record.partition(), record.offset(), record.key(), e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<?> dispatch(ConsumerRecord<String, byte[]> record, Event event,
                                          AsyncEventConsumer eventConsumer) {
        String previousDeliveredFrom = MDC.get(MDC_DELIVERED_FROM);
        MDC.put(MDC_DELIVERED_FROM, transportName + "-" + record.topic() + "-p" + record.partition());
        Map<String, String> context = MDC.getCopyOfContextMap();
        try {
            CompletableFuture<?> processing = Objects.requireNonNull(eventConsumer.accept(event),
                    "AsyncEventConsumer.accept() must not return null; return a completed future "
                            + "for synchronous processing");
            CompletableFuture<?> tracked = processing.whenComplete((result, error) -> {
                if (error != null) {
                    runWithMdc(context, () -> log.error(
                            "Event processing failed for topic: {}, partition: {}, offset: {} — "
                                    + "the record will be re-delivered after restart",
                            record.topic(), record.partition(), record.offset(), error));
                }
            });
            if (log.isDebugEnabled()) {
                log.debug("Event delivered from Kafka topic: {}, key: {}, event type: {}, payload type: {}",
                        record.topic(), record.key(), event.type().getSimpleName(), payloadType(event));
            }
            return tracked;
        } catch (Exception e) {
            // Synchronous consumer failure: the record must be re-delivered,
            // so its offset must not advance.
            log.error("Event delivery failed for topic: {}, partition: {}, offset: {} — "
                            + "offset will not be committed; the record will be re-delivered after restart",
                    record.topic(), record.partition(), record.offset(), e);
            return CompletableFuture.failedFuture(e);
        } finally {
            restoreMdc(previousDeliveredFrom);
        }
    }

    private static String payloadType(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload() == null ? "null" : envelope.payload().getClass().getSimpleName();
        }
        return event.getClass().getSimpleName();
    }

    /**
     * Runs the action with the captured MDC context so asynchronous callbacks
     * keep the delivery diagnostics.
     *
     * @param context captured MDC context, may be null
     * @param action  the action to run
     */
    private static void runWithMdc(Map<String, String> context, Runnable action) {
        Map<String, String> previous = MDC.getCopyOfContextMap();
        try {
            if (context != null) {
                MDC.setContextMap(context);
            } else {
                MDC.clear();
            }
            action.run();
        } finally {
            if (previous != null) {
                MDC.setContextMap(previous);
            } else {
                MDC.clear();
            }
        }
    }

    /**
     * Restores the previous value of the delivery MDC key.
     *
     * @param previousDeliveredFrom the previous value, or null when absent
     */
    private static void restoreMdc(String previousDeliveredFrom) {
        if (previousDeliveredFrom != null) {
            MDC.put(MDC_DELIVERED_FROM, previousDeliveredFrom);
        } else {
            MDC.remove(MDC_DELIVERED_FROM);
        }
    }
}
