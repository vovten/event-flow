# Handler Registry Specification

## Purpose

Handler registries store and resolve event handlers for dispatch. This spec defines the `EventHandlerRegistry` contract, annotation-based and interface-based registration, the composite registry, and handler resolution semantics.

## Requirements

### Requirement: Registry Contract
An event handler registry SHALL resolve the list of handlers for an event type, register and unregister handler objects, report whether an object is registered, merge with other registries, and expose a handler count and a name. Resolution SHALL never return null.

#### Scenario: Resolve handlers for an event
- GIVEN a registry with a handler for a specific event type
- WHEN handlers are resolved for that event type
- THEN a non-null list SHALL be returned
- AND the list SHALL contain the registered handler

#### Scenario: No handlers for unknown type
- GIVEN a registry with no handlers for an event type
- WHEN handlers are resolved for that type
- THEN an empty non-null list SHALL be returned

### Requirement: Annotation-Based Registration
The registry SHALL register objects by scanning public methods annotated with `@EventListener`. A listener method SHALL have exactly one parameter whose type is an event, an envelope, or a payload annotated with `@Event`. Methods with invalid signatures SHALL be rejected at registration time.

#### Scenario: Register annotated method
- GIVEN an object with a public method annotated with `@EventListener` accepting a specific event type
- WHEN the object is registered
- THEN the method SHALL be available as a handler for that event type

#### Scenario: Invalid signature rejected
- GIVEN an object with an `@EventListener` method that has zero or multiple parameters
- WHEN the object is registered
- THEN an `InvalidEventListenerMethodSignatureException` SHALL be thrown

#### Scenario: Envelope requires explicit type
- GIVEN an `@EventListener` method whose single parameter is an `Envelope` without an explicit event type
- WHEN the object is registered
- THEN an `IllegalArgumentException` SHALL be thrown

#### Scenario: Duplicate registration ignored
- GIVEN an object already registered
- WHEN the same object and method are registered again
- THEN the duplicate SHALL be ignored
- AND the handler SHALL appear exactly once

### Requirement: Event Handler Resolution
The registry SHALL resolve handlers for an event by matching the exact event class and generic handlers registered for the root `Event` class. For envelope events, resolution SHALL use the payload class.

#### Scenario: Exact and generic handlers combined
- GIVEN a registry with a type-specific handler and a generic handler for `Event`
- WHEN handlers are resolved for a concrete event
- THEN the list SHALL contain both the generic and the type-specific handlers

#### Scenario: Envelope resolution by payload type
- GIVEN a registry with a handler for a payload class
- WHEN handlers are resolved for an envelope wrapping that payload
- THEN the handler SHALL be included

### Requirement: Handler Invocation Adaptation
When a registered listener method accepts a payload type but receives an envelope, the invocation SHALL pass the envelope's payload to the method. When the method accepts an envelope, the envelope SHALL be passed unchanged.

#### Scenario: Payload unwrapping
- GIVEN a listener method accepting a payload class
- WHEN an envelope wrapping that payload is dispatched
- THEN the method SHALL be invoked with the payload, not the envelope

#### Scenario: Invocation failure wrapped
- GIVEN a listener method that throws during invocation
- WHEN the method is invoked
- THEN the failure SHALL be wrapped in an invocation exception that names the listener and the event

### Requirement: Interface-Based Registration
The registry SHALL register objects that implement the `EventSubscriber` interface, which declares the event types it handles. Objects that do not implement the interface SHALL be silently ignored.

#### Scenario: Register interface subscriber
- GIVEN an object implementing `EventSubscriber` declaring specific event types
- WHEN the object is registered
- THEN it SHALL be available as a handler for each declared type

#### Scenario: Generic subscriber receives all
- GIVEN a subscriber whose declared types include the root `Event` class
- WHEN handlers are resolved for any event
- THEN the subscriber SHALL be included

#### Scenario: Non-subscriber ignored
- GIVEN an object that does not implement `EventSubscriber`
- WHEN the object is registered
- THEN the object SHALL be silently ignored
- AND no handlers SHALL be registered

### Requirement: Composite Registry
The composite registry SHALL delegate operations to a list of child registries in the order they were provided. Resolving handlers SHALL concatenate results from all children. Unregistering SHALL succeed if any child removed the handler. Merging a composite registry SHALL add its children; merging another registry SHALL add it as a child.

#### Scenario: Resolution across children
- GIVEN a composite registry with two children each holding a handler for the same event type
- WHEN handlers are resolved
- THEN the result SHALL contain handlers from both children in child order

#### Scenario: Unregister across children
- GIVEN a composite registry where a handler is registered in only one child
- WHEN the handler is unregistered
- THEN the unregister call SHALL return true

#### Scenario: Empty composite rejected
- GIVEN a composite registry constructed with a null or empty child list
- WHEN it is constructed
- THEN an `IllegalArgumentException` SHALL be thrown

### Requirement: Registry Builder
The system SHALL provide a fluent builder for building registries from annotation listeners, interface listeners, custom registries, and decorators. Building without any configured source SHALL fail.

#### Scenario: Build requires a source
- GIVEN a registry builder with no annotation listeners, no interface listeners, and no custom registries
- WHEN the registry is built
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Composite when multiple sources
- GIVEN a registry builder with both annotation and interface listeners enabled
- WHEN the registry is built
- THEN a composite registry SHALL be produced containing both child registries

#### Scenario: Single source not wrapped
- GIVEN a registry builder with only annotation listeners enabled
- WHEN the registry is built
- THEN a single annotation registry SHALL be produced without a composite wrapper
