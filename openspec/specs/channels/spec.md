# Event Channels Specification

## Purpose

Event channels define logical routes that carry events from publishers to outgoing transports. This spec defines the `EventChannel` contract, the built-in internal/external/broadcast channels, and the delivery semantics including failure isolation.

## Requirements

### Requirement: Channel Contract
An event channel SHALL expose a unique name and the list of outgoing transports it delivers to. Sending an event SHALL dispatch it to all configured transports and SHALL return a future that completes when all transports have finished.

#### Scenario: Channel delivery
- GIVEN a channel with two outgoing transports
- WHEN an event is sent through the channel
- THEN both transports SHALL receive the event
- AND the returned future SHALL complete when both transports have finished

### Requirement: Channel Requires Transport
A channel SHALL be constructed with at least one outgoing transport. Constructing a channel without transports or with a null transport SHALL fail.

#### Scenario: Empty transport list rejected
- GIVEN a channel constructor call with a null or empty transport list
- WHEN the channel is constructed
- THEN an `IllegalArgumentException` SHALL be thrown

#### Scenario: Null transport rejected
- GIVEN a channel constructor call with a null transport
- WHEN the channel is constructed
- THEN an `IllegalArgumentException` SHALL be thrown

### Requirement: Transport Failure Isolation
A channel SHALL NOT propagate transport failures to the caller as exceptional futures. If a transport fails to send an event, the channel SHALL record a failure result for that transport and continue with the remaining transports. The aggregated send results SHALL always complete normally.

#### Scenario: Single transport failure
- GIVEN a channel with two transports where the first one fails
- WHEN an event is sent through the channel
- THEN the returned future SHALL complete normally
- AND the results SHALL contain one success and one failure
- AND the failure SHALL reference the failed transport name and the underlying cause

#### Scenario: All transports fail
- GIVEN a channel where every transport fails
- WHEN an event is sent through the channel
- THEN the returned future SHALL still complete normally
- AND the results SHALL indicate an all-failure outcome

### Requirement: Internal Channel
The system SHALL provide an internal channel for in-application delivery. Its name SHALL be `"internal"`.

#### Scenario: Internal channel identity
- GIVEN an internal event channel
- WHEN its name is accessed
- THEN the value `"internal"` SHALL be returned

### Requirement: External Channel
The system SHALL provide an external channel for cross-application delivery through a message broker. Its name SHALL be `"external"`.

#### Scenario: External channel identity
- GIVEN an external event channel
- WHEN its name is accessed
- THEN the value `"external"` SHALL be returned

### Requirement: Broadcast Channel
The system SHALL provide a broadcast channel that delivers an event to all configured transports. A failure of one transport SHALL NOT prevent delivery to the others. With a single transport it SHALL behave like an ordinary channel. Its name SHALL be `"broadcast"`.

#### Scenario: Broadcast to all transports
- GIVEN a broadcast channel with multiple transports where one fails
- WHEN an event is sent through the channel
- THEN the remaining transports SHALL still receive the event
- AND the aggregated results SHALL contain both success and failure entries

#### Scenario: Broadcast identity
- GIVEN a broadcast event channel
- WHEN its name is accessed
- THEN the value `"broadcast"` SHALL be returned
