/**
 * Internal collaborators of the inbound Kafka transport.
 * <p>
 * The public entry point is
 * {@link io.github.vovten.eventflow.transport.incoming.KafkaInTransport}; the
 * classes in this package split its responsibilities:
 * <ul>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.kafka.PartitionStateTracker}
 *       — per-partition record state, offset frontier and failure blocks;</li>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.kafka.KafkaOffsetCommitter}
 *       — confirm-based offset commits;</li>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.kafka.KafkaRebalanceListener}
 *       — commit-on-revoke and state cleanup;</li>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.kafka.BackpressureController}
 *       — pause/resume based on in-flight and buffered counts;</li>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.kafka.PartitionUnblockQueue}
 *       — deferred manual partition release;</li>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.kafka.KafkaRecordDelivery}
 *       — deserialization, poison-pill/tombstone handling and MDC propagation;</li>
 *   <li>{@link io.github.vovten.eventflow.transport.incoming.kafka.KafkaConsumerProperties}
 *       — at-least-once consumer defaults and topic parsing.</li>
 * </ul>
 * These types are implementation details and are not intended as a stable
 * public API.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
package io.github.vovten.eventflow.transport.incoming.kafka;
