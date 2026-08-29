# Channels

## Purpose

Define how events are routed through logical channels to outgoing transports. Channels decouple event publishers from specific transport mechanisms, allowing flexible and cross-cutting configuration of event delivery while keeping the publisher unaware of transport details.

## Requirements

### Requirement: Channel abstraction

A channel represents a logical route for event delivery. It decouples the publisher from the concrete transport mechanism and exposes a uniform send contract.

#### Scenario: Sending an event through a channel

- **WHEN** an application sends an event to a channel
- **THEN** the channel forwards the event to every transport configured on it
- **THEN** the returned future completes with the aggregated `SendResults` of all transport sends

#### Scenario: Providing channel identity

- **WHEN** a channel is registered in the system
- **THEN** it exposes a unique channel name (such as "internal" or "external") for identification and configuration

#### Scenario: Declaring configured transports

- **WHEN** an application inspects a channel
- **THEN** the channel returns the list of outgoing transports it is configured with

### Requirement: Channel types

The framework provides built-in channel types covering internal, external, and broadcast routing, each suited to a specific delivery scenario.

#### Scenario: Routing within the application

- **WHEN** an event is sent to an internal channel
- **THEN** the event is delivered through the internally configured transports for in-application processing

#### Scenario: Routing to external systems

- **WHEN** an event is sent to an external channel
- **THEN** the event is delivered through the external transports for delivery to other applications or services

#### Scenario: Broadcasting to all partitions

- **WHEN** an event is sent to a broadcast channel
- **THEN** the event is delivered to every partition of the configured broadcast destination

### Requirement: Aggregated send results

The outcome of delivering to multiple transports is represented as an aggregate that applications can inspect to determine overall delivery success.

#### Scenario: All sends succeed

- **WHEN** every transport in a channel reports a successful send
- **THEN** the aggregated `SendResults` reports all sends as successful

#### Scenario: Some sends fail

- **WHEN** at least one transport succeeds and at least one fails
- **THEN** the aggregated `SendResults` reports a partial success
- **THEN** the failed results are individually accessible with their error details

#### Scenario: All sends fail

- **WHEN** every transport in a channel reports a failure
- **THEN** the aggregated `SendResults` reports an all-failure state

### Requirement: Channel construction

Applications assemble channels and attach transports to them, either through the constructor of a built-in channel or through a dedicated transport builder.

#### Scenario: Building a channel with transports

- **WHEN** an application creates a channel and attaches one or more outgoing transports
- **THEN** the channel owns those transports and routes events through all of them

#### Scenario: Building a paired in-process transport

- **WHEN** an application builds a local-queue transport pair through the local-queue transports builder
- **THEN** the builder returns an outgoing publisher transport and an incoming dispatcher transport that share the same in-process queue

### Requirement: Transport-independence of publishers

Publishers route to channels, never to concrete transports directly. This keeps the publishing layer decoupled from the underlying delivery mechanism.

#### Scenario: Swapping a transport without changing the publisher

- **WHEN** the set of transports attached to a channel is replaced
- **THEN** the publisher continues to route through the channel without requiring any change to its configuration