# Serialization

## Purpose

Define how events are serialized for the wire and deserialized on receipt, including the magic-byte format negotiation, built-in and custom serializers, automatic format detection, and the security whitelist that gates which classes may be deserialized.

## Requirements

### Requirement: Serialization contract

Every serializer converts an event to and from a byte array prefixed with a single format code (magic byte) that identifies the serialization format.

#### Scenario: Serializing an event

- **WHEN** an application serializes an event through a serializer
- **THEN** the result is a byte array whose first byte is the serializer's format code and whose remaining bytes are the serialized event data
- **THEN** a failure during serialization raises a serialization exception

#### Scenario: Deserializing an event

- **WHEN** an application deserializes bytes for a known event type
- **THEN** the serializer reconstructs and returns the event
- **THEN** a failure during deserialization raises a serialization exception

#### Scenario: Identifying a serializer

- **WHEN** an application inspects a serializer
- **THEN** it exposes a unique format code and a unique configuration name

### Requirement: Reserved and custom format codes

Format codes partition the space so built-in formats are identifiable and custom formats can be added without collision.

#### Scenario: Using built-in formats

- **WHEN** data is serialized with one of the built-in serializers
- **THEN** JSON uses the reserved code `0x01` and MessagePack uses the reserved code `0x02`

#### Scenario: Registering a custom format

- **WHEN** an application registers a custom serializer with its own format code
- **THEN** the serializer becomes available under its name and code for later use

### Requirement: Format lookup and detection

The serializer factory resolves serializers both by explicit configuration name and automatically from the leading byte of data during deserialization.

#### Scenario: Resolving a serializer by name

- **WHEN** an application requests a serializer by its configured name
- **THEN** the factory returns the matching serializer
- **THEN** an unknown name raises a serialization exception

#### Scenario: Resolving a serializer by data

- **WHEN** an application passes serialized data to the factory for resolution
- **THEN** the factory detects the format from the leading byte and returns the matching serializer

#### Scenario: Detecting the legacy JSON format

- **WHEN** deserialized data begins with the JSON object opening byte (without a magic byte)
- **THEN** the format is treated as the legacy JSON format for backward compatibility

#### Scenario: Rejecting empty data

- **WHEN** an application attempts to resolve a serializer from empty data
- **THEN** a serialization exception is raised

### Requirement: Built-in serializers

The framework ships JSON and MessagePack serializers and exposes a default serializer for unconfigured use.

#### Scenario: Using the default serializer

- **WHEN** an application requests the default serializer
- **THEN** the JSON serializer is returned

#### Scenario: Accessing a specific built-in serializer

- **WHEN** an application requests the JSON or MessagePack serializer by name
- **THEN** the corresponding built-in serializer is returned

### Requirement: Whitelist-gated deserialization

Deserialization is restricted to a configurable whitelist of allowed packages and classes as a defense against deserialization attacks.

#### Scenario: Allowing a package

- **WHEN** an application allows a package in the type registry
- **THEN** all event classes under that package become deserializable

#### Scenario: Allowing a specific class

- **WHEN** an application allows a specific event class in the type registry
- **THEN** that class becomes deserializable

#### Scenario: Allowing the framework package by default

- **WHEN** the registry is first used
- **THEN** the framework's own package is allowed by default so its events remain deserializable

#### Scenario: Rejecting a non-allowed class

- **WHEN** an event class is neither explicitly allowed nor within an allowed package
- **THEN** the registry reports that the class is not allowed for deserialization

#### Scenario: Rejecting invalid registry configuration

- **WHEN** an application attempts to allow a null package or a null class
- **THEN** an `IllegalArgumentException` is raised