# Handler Registry

## Purpose

Define how event handlers are discovered, registered, and resolved for dispatch. The registry is the centralized mechanism that stores handlers and matches them to events by type, supporting both annotation-based and interface-based handler discovery, composition of multiple registries, dynamic registration, and lifecycle of registered handlers.

## Requirements

### Requirement: Annotation-based handler discovery

Handlers can be declared by marking methods with the `@EventListener` annotation; the registry discovers and binds these methods to the event types they accept.

#### Scenario: Registering an annotation-based handler

- **WHEN** an object with methods annotated `@EventListener` is registered in the registry
- **THEN** each annotated method is bound to the event class it accepts
- **THEN** those methods are eligible to handle events of that type when dispatched

#### Scenario: Rejecting a malformed handler method

- **WHEN** a registered annotated method does not have a valid event-handler signature
- **THEN** registration fails with an `InvalidEventListenerMethodSignatureException`

### Requirement: Interface-based handler discovery

Handlers can alternatively implement the `EventSubscriber` interface and declare the event types they handle.

#### Scenario: Registering an interface-based handler

- **WHEN** an object implementing `EventSubscriber` is registered in the registry
- **THEN** the handler is bound to the event types declared by its `events()` method
- **THEN** those events are delivered to the handler when dispatched

### Requirement: Handler resolution by event type

The registry returns the set of handlers applicable to a given event so the dispatcher can deliver it to each of them.

#### Scenario: Resolving handlers for an event

- **WHEN** the dispatcher queries the registry for an event
- **THEN** the registry returns all handlers registered for that event's type
- **THEN** the returned list is never null

### Requirement: Dynamic registration lifecycle

Handlers can be registered and unregistered at runtime, and their registration can be queried.

#### Scenario: Registering a handler

- **WHEN** an application registers a handler in the registry
- **THEN** the handler becomes eligible to receive the event types it handles

#### Scenario: Unregistering a handler

- **WHEN** an application unregisters a handler that was registered
- **THEN** the handler is removed and no longer receives events
- **THEN** the call reports that the handler was found and removed

#### Scenario: Querying registration status

- **WHEN** an application checks whether a handler is registered
- **THEN** the registry reports true if the handler is currently registered and false otherwise

### Requirement: Registry composition

Multiple registries can be combined into a single composite registry, so handlers discovered through different strategies are resolved together.

#### Scenario: Combining multiple registries

- **WHEN** a registry is built from multiple strategy registries (annotation-based, interface-based, or custom)
- **THEN** the combined registry resolves handlers from all of them as a single source

#### Scenario: Merging another registry

- **WHEN** an application merges another handler registry into an existing one
- **THEN** the handlers of the merged registry become part of the receiving registry

### Requirement: Registry construction

A registry is assembled from one or more handler sources and optional decorators, and configuration errors are detected at build time.

#### Scenario: Building a registry without a handler source

- **WHEN** an application calls `build()` on a registry builder that has no handler source configured
- **THEN** an `IllegalStateException` is thrown and no registry is created

#### Scenario: Building a single-source registry

- **WHEN** a registry builder has exactly one handler source configured
- **THEN** the built registry is that single source registry, with no extra composition overhead

#### Scenario: Decorating a registry

- **WHEN** an application registers decorators on the registry builder
- **THEN** each decorator is applied around the built registry

### Requirement: Counting registered handlers

The registry reports an aggregate count of the handlers it holds.

#### Scenario: Reporting the handler count

- **WHEN** an application requests the handler count
- **THEN** the registry returns the total number of registered handlers it holds