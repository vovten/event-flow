package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.transport.PublisherTransport;
import java.util.List;

/**
 * Broadcast channel for delivering events to multiple consumers simultaneously.
 * <p>
 * This channel is used when an event needs to be delivered to all subscribers
 * or multiple external systems. Events published to this channel are sent through
 * all configured transports, enabling parallel distribution to different messaging
 * systems or multiple instances of the same system.
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Publishing domain events that multiple services need to react to</li>
 *   <li>Sending notifications to multiple external systems at once</li>
 *   <li>Event broadcasting in microservice architectures</li>
 *   <li>Simultaneous delivery to different message brokers (e.g., Kafka and RabbitMQ)</li>
 *   <li>Fan-out scenarios where each consumer should receive a copy of the event</li>
 *   <li>Kubernetes environments where all pods of a service should receive the event</li>
 * </ul>
 * <p>
 * <b>Key characteristics:</b>
 * <ul>
 *   <li>Events are sent to ALL configured transports</li>
 *   <li>Each transport operates independently - failure in one doesn't affect others</li>
 *   <li>Ideal for publish-subscribe patterns</li>
 *   <li>Supports hybrid messaging infrastructure</li>
 * </ul>
 * <p>
 * <b>Configuration examples:</b>
 * <pre>{@code
 * // Broadcasting to multiple Kafka clusters
 * EventChannel broadcastChannel = new BroadcastEventChannel(List.of(
 *     new KafkaPublisherTransport("cluster1:9092", "events-topic"),
 *     new KafkaPublisherTransport("cluster2:9092", "events-topic")
 * ));
 *
 * // Broadcasting to different message brokers
 * EventChannel hybridChannel = new BroadcastEventChannel(List.of(
 *     new KafkaPublisherTransport("localhost:9092", "domain-events"),
 *     new RabbitMqPublisherTransport("localhost:5672", "events-exchange")
 * ));
 *
 * // Broadcasting to all pods in a Kubernetes deployment
 * // When used with a transport that supports Kubernetes headless services,
 * // all pods in the deployment will receive a copy of the event
 * EventChannel k8sBroadcastChannel = new BroadcastEventChannel(
 *     new KafkaPublisherTransport("kafka-headless.kafka.svc.cluster.local:9092", "pod-events")
 * );
 *
 * // Single transport (acts like a regular channel)
 * EventChannel simpleChannel = new BroadcastEventChannel(
 *     new KafkaPublisherTransport("localhost:9092", "events")
 * );
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-12
 */
public class BroadcastEventChannel extends AbstractEventChannel {

    /**
     * Creates a broadcast channel with multiple transports.
     * <p>
     * Events published to this channel will be broadcasted through all
     * provided transports simultaneously.
     *
     * @param transports list of transports for broadcasting events
     */
    public BroadcastEventChannel(List<PublisherTransport> transports) {
        super(transports);
    }

    /**
     * Creates a broadcast channel with a single transport.
     * <p>
     * While technically not broadcasting, this constructor provides consistency
     * when you need to treat all channels uniformly.
     *
     * @param transport the transport for this channel
     */
    public BroadcastEventChannel(PublisherTransport transport) {
        super(transport);
    }

    @Override
    public String name() {
        return "broadcast";
    }
}