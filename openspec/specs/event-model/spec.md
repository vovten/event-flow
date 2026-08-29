# Event Model

## Purpose

Define the core event types of the framework: the base `Event` contract, the `Envelope` that wraps payloads with technical metadata, the `TraceableEvent` marker for traceable events, and the resolution rules for channels and lifecycle level.

## Requirements

### Requirement: Event contract

An event is an immutable fact about something that happened in the system, and every event exposes its type and its routing targets.

#### Scenario: Exposing the event type

- **WHEN** an application inspects an event
- **THEN** the event exposes its type class for dispatch and serialization

#### Scenario: Declaring default routing

- **WHEN** an event does not override its channel declaration
- **THEN** the event is routed to the internal channel by default

#### Scenario: Declaring multiple channels

- **WHEN** an event overrides its channel declaration with several channel classes
- **THEN** the event is routed to all of those channels

### Requirement: Traceable event contract

Events can extend the traceable contract to carry a unique identifier, an optional process correlation identifier, and a timestamp for end-to-end tracking.

#### Scenario: Providing traceability fields

- **WHEN** an event implements the traceable contract
- **THEN** it exposes a unique event identifier, a process correlation identifier, and an occurrence timestamp

### Requirement: Envelope wrapping

Plain objects and events can be wrapped in an `Envelope` that adds technical metadata while remaining a valid `Event` so it flows through the existing infrastructure unchanged.

#### Scenario: Wrapping a payload in an envelope

- **WHEN** a payload is wrapped using a standard envelope factory
- **THEN** the envelope carries a generated event identifier, a null process identifier, and the current timestamp
- **THEN** the envelope behaves as a regular event for routing and dispatch

#### Scenario: Wrapping a payload with an explicit identifier

- **WHEN** a payload is wrapped with an explicit event identifier and optional process identifier
- **THEN** the envelope carries those identifiers instead of generating them

#### Scenario: Wrapping a payload with explicit channels

- **WHEN** a payload is wrapped with an explicit channel list
- **THEN** those channels take priority over the payload's own channel resolution

#### Scenario: Attaching metadata

- **WHEN** application code adds metadata to an envelope
- **THEN** a new envelope is produced containing the merged metadata

#### Scenario: Rejecting a nested envelope

- **WHEN** a payload that is itself an envelope is wrapped
- **THEN** an `IllegalArgumentException` is raised, preventing nested envelopes

#### Scenario: Rejecting a null payload

- **WHEN** a null payload is wrapped
- **THEN** an `IllegalArgumentException` is raised

### Requirement: Channel resolution for envelopes

When no explicit channels are given, an envelope resolves channels from its payload deterministically.

#### Scenario: Resolving channels from an annotation

- **WHEN** the wrapped payload carries a channel declaration annotation
- **THEN** the envelope is routed to the annotated channels

#### Scenario: Resolving channels from the event interface

- **WHEN** the payload implements the event interface but has no annotation
- **THEN** the envelope is routed to the channels declared by the payload's event interface override

#### Scenario: Falling back to the internal channel

- **WHEN** the payload is a plain object with neither an annotation nor an event interface override
- **THEN** the envelope is routed to the internal channel

### Requirement: Configuring publishing metadata via annotation

Payload classes can be annotated to provide default routing and lifecycle configuration used when they are wrapped.

#### Scenario: Declaring metadata on a payload

- **WHEN** a payload class is annotated with a publishing-metadata annotation
- **THEN** that annotation provides default target channels and a lifecycle level for the wrapped event
- **THEN** explicit factory parameters override the annotation values when both are present

### Requirement: Event building

Applications construct events with fine-grained metadata using an event builder instead of factory methods.

#### Scenario: Building an event through a builder

- **WHEN** an application builds an event through the event builder
- **THEN** it can set the process identifier and additional metadata before publishing
- **THEN** calling publish on the builder publishes the fully configured envelope