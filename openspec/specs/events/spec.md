# Events Specification

## Purpose

Event Flow models application facts as immutable event objects that are routed through event channels to publishers and dispatchers. This spec defines the event model: the core `Event` contract, traceable events, the `Envelope` wrapper, event builders, and the `@Event` annotation.

## Requirements

### Requirement: Event Contract
The system SHALL treat events as the basic unit of communication. Every event SHALL expose its concrete runtime type via `type()` and SHALL declare the set of event channels it is routed through via `channels()`.

#### Scenario: Default channel routing
- GIVEN an event implementation that does not override `channels()`
- WHEN the event is published
- THEN it SHALL be routed to the `InternalEventChannel`
- AND the channel list SHALL contain exactly the internal channel class

#### Scenario: Default lifecycle level
- GIVEN an event implementation that does not override `lifecycle()` and has no `@Event` annotation
- WHEN the event is published
- THEN its lifecycle level SHALL be `PERSISTED`

#### Scenario: Annotation overrides lifecycle
- GIVEN an event class annotated with `@Event(lifecycle = MANAGED)`
- WHEN the lifecycle level is resolved
- THEN the annotation value SHALL take precedence over the `Event.lifecycle()` default

### Requirement: JSON Type Metadata
The system SHALL serialize the concrete event type into JSON so that deserialization can restore the original class. Event classes SHALL be annotated with Jackson `@JsonTypeInfo(use = Id.CLASS)`.

#### Scenario: Polymorphic round-trip
- GIVEN an event serialized to JSON
- WHEN the JSON is deserialized back
- THEN the resulting object SHALL be an instance of the original concrete event class

### Requirement: Traceable Events
Traceable events SHALL carry a unique event identifier, an optional correlation process identifier, and the instant the event occurred. Events that are traceable SHALL expose these via `eventId()`, `processId()`, and `occurredAt()`.

#### Scenario: Event identity
- GIVEN a traceable event
- WHEN `eventId()` is accessed
- THEN a non-null UUID SHALL be returned

#### Scenario: Optional process correlation
- GIVEN a traceable event created without a process identifier
- WHEN `processId()` is accessed
- THEN `null` SHALL be returned

### Requirement: Envelope Wrapper
The system SHALL provide an `Envelope` that wraps a payload event and adds technical metadata: event identifier, optional process identifier, occurrence timestamp, and an immutable string-to-string metadata map. The envelope SHALL itself implement `Event` so it can pass through transports unchanged.

#### Scenario: Envelope construction
- GIVEN a payload that is not null and not itself an envelope
- WHEN an envelope is created with a random event id
- THEN the event id SHALL be non-null
- AND the occurrence timestamp SHALL be set to the current time
- AND the metadata map SHALL be empty

#### Scenario: Rejecting nested envelopes
- GIVEN a payload that is itself an `Envelope`
- WHEN an envelope is created from it
- THEN an `IllegalArgumentException` SHALL be thrown

#### Scenario: Envelope channel resolution
- GIVEN an envelope whose payload carries an `@Event` annotation with channels
- WHEN the envelope is created without explicit channels
- THEN the envelope SHALL route through the channels declared by the payload annotation

#### Scenario: Explicit channels win
- GIVEN a payload whose annotation declares internal routing
- WHEN an envelope is created with explicit external channels
- THEN the explicit channels SHALL take precedence over the annotation
- AND the channel list SHALL be treated as immutable

#### Scenario: Immutable metadata
- GIVEN an envelope constructed with a mutable metadata map
- WHEN the metadata is read from the envelope
- THEN the envelope SHALL expose an immutable copy
- AND further mutation of the source map SHALL NOT affect the envelope

#### Scenario: Additional metadata
- GIVEN an existing envelope with metadata
- WHEN `withAdditionalMetadata` is invoked
- THEN a new envelope SHALL be returned
- AND the original envelope SHALL remain unchanged
- AND the new envelope SHALL contain the union of old and new metadata

#### Scenario: Envelope equality by id
- GIVEN two envelopes with the same event identifier but different payloads
- WHEN they are compared with `equals`
- THEN they SHALL be considered equal

### Requirement: Event Builder
The system SHALL provide a fluent event builder that constructs an envelope and optionally publishes it through an associated publisher. The builder SHALL accept a process identifier, an occurrence timestamp, metadata entries, and an optional channel list.

#### Scenario: Builder defaults
- GIVEN a builder created for a payload
- WHEN the builder is used without setting an occurrence timestamp
- THEN the occurrence timestamp SHALL default to the current time
- AND the metadata SHALL be empty

#### Scenario: Builder rejects null process id
- GIVEN a builder
- WHEN `withProcessId(null)` is invoked
- THEN a `NullPointerException` SHALL be thrown

#### Scenario: Builder channel validation
- GIVEN a builder
- WHEN an empty or null channel list is supplied
- THEN an `IllegalArgumentException` SHALL be thrown

#### Scenario: Publish through associated publisher
- GIVEN a builder configured with an event publisher and metadata
- WHEN `publish()` is invoked
- THEN an envelope SHALL be constructed from the configured values
- AND the publisher SHALL be invoked with that envelope
- AND a future of send results SHALL be returned

### Requirement: Deprecated Abstract Traceable Event
The system SHALL NOT require event authors to extend a base traceable class. The `AbstractTraceableEvent` base class SHALL remain available for compatibility but SHALL be marked deprecated.

#### Scenario: Backward compatibility
- GIVEN an existing event class extending the deprecated base class
- WHEN the class is compiled
- THEN it SHALL continue to work without modification

### Requirement: @Event Annotation
The system SHALL provide an `@Event` annotation that marks plain Java objects and records as event payloads and configures publication metadata: routing channels and lifecycle level.

#### Scenario: Annotation routing
- GIVEN a payload class annotated with `@Event(channels = ExternalEventChannel.class)`
- WHEN the payload is published without explicit channels
- THEN it SHALL be routed to the external channel

#### Scenario: Annotation lifecycle
- GIVEN a payload class annotated with `@Event(lifecycle = MANAGED)`
- WHEN the lifecycle level is resolved
- THEN the annotated lifecycle SHALL be used
