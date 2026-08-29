# Transport

## Purpose

Define the transport layer that delivers events to destinations and receives them from sources, including the outgoing and incoming transport contracts, the built-in local-queue and Kafka transports, per-send results, and a paired in-process local-queue setup.

## Requirements

### Requirement: Outgoing transport contract

An outgoing transport delivers an event to a concrete destination and reports a per-send result.

#### Scenario: Sending an event to a destination

- **WHEN** an outgoing transport is asked to send an event
- **THEN** the transport delivers the event to its destination asynchronously
- **THEN** the returned future completes with a `SendResult` describing the outcome

#### Scenario: Identifying an outgoing transport

- **WHEN** an application inspects an outgoing transport
- **THEN** it exposes a unique transport name (such as "kafka" or "local-queue")

### Requirement: Incoming transport contract

An incoming transport receives events from a source and delivers them to a consumer.

#### Scenario: Receiving events from a source

- **WHEN** an incoming transport is started with a consumer
- **THEN** it begins receiving events from its source and delivers each one to the consumer

#### Scenario: Stopping an incoming transport

- **WHEN** an incoming transport is stopped
- **THEN** it gracefully shuts down and releases its resources

#### Scenario: Identifying an incoming transport

- **WHEN** an application inspects an incoming transport
- **THEN** it exposes a unique transport name

### Requirement: Per-send result

Each send produces a result describing its success and the destination it targeted.

#### Scenario: Reporting a successful send

- **WHEN** an outgoing transport successfully delivers an event
- **THEN** it reports a successful result for the destination, including a timestamp and optional message identifier

#### Scenario: Reporting a failed send

- **WHEN** an outgoing transport fails to deliver an event
- **THEN** it reports a failed result carrying the destination and the error details

### Requirement: Local-queue transport

The local-queue transport provides in-process event communication through a bounded blocking queue.

#### Scenario: Communicating in-process via a shared queue

- **WHEN** an outgoing local-queue transport and an incoming local-queue transport share the same blocking queue
- **THEN** events sent by the outgoing transport are received by the incoming transport from that queue

#### Scenario: Building a paired local-queue setup

- **WHEN** an application builds a local-queue transport pair through the dedicated builder
- **THEN** it receives an outgoing publisher transport and an incoming dispatcher transport sharing one queue
- **THEN** the builder honours a requested queue size or an explicitly provided queue and executor

### Requirement: Kafka transport

Kafka transports provide distributed event delivery and consumption against a Kafka cluster.

#### Scenario: Sending to a Kafka topic

- **WHEN** an outgoing Kafka transport is configured with bootstrap servers and a topic
- **THEN** events sent through it are delivered to that topic

#### Scenario: Broadcasting to all partitions

- **WHEN** an outgoing broadcast-Kafka transport is used
- **THEN** each event is delivered to every partition of the configured topic

#### Scenario: Consuming from Kafka topics

- **WHEN** an incoming Kafka transport is started with bootstrap servers, topics, and a consumer group
- **THEN** it consumes events from those topics on behalf of the group and delivers them to the consumer