# Serialization Specification

## Purpose

Serialization converts events to and from wire formats for transport. This spec defines the serializer contract, the built-in JSON and MessagePack formats, format detection, the serializer factory, and the security controls for polymorphic deserialization.

## Requirements

### Requirement: Serializer Contract
A serializer SHALL convert an event to bytes and back. Serialized output SHALL begin with a magic byte that identifies the format code. Deserialization SHALL accept the target event type and SHALL detect the format from the first byte.

#### Scenario: Serialize event
- GIVEN a serializer and an event
- WHEN the event is serialized
- THEN a byte array SHALL be returned
- AND the first byte SHALL equal the serializer's format code

#### Scenario: Deserialize event
- GIVEN a serializer
- WHEN bytes produced by that serializer are deserialized with the target type
- THEN the original event SHALL be restored

#### Scenario: Serialization error
- GIVEN a serializer and an event that cannot be serialized
- WHEN serialization is attempted
- THEN a serialization exception SHALL be thrown

### Requirement: JSON Format
The system SHALL provide a JSON serializer identified by name `"json"` and code `0x01`. JSON output SHALL be text-encoded UTF-8, SHALL include the concrete event type for polymorphic deserialization, SHALL serialize dates as ISO-8601 strings, SHALL ignore unknown properties on deserialization, and SHALL accept legacy JSON without a magic byte for backward compatibility.

#### Scenario: JSON format code
- GIVEN the JSON serializer
- WHEN its code is accessed
- THEN the value `0x01` SHALL be returned

#### Scenario: JSON round-trip
- GIVEN a JSON serializer and an event
- WHEN the event is serialized to JSON and deserialized back
- THEN the original event SHALL be restored with its concrete type

#### Scenario: Legacy JSON without magic byte
- GIVEN JSON bytes without a magic byte that begin with an opening brace
- WHEN the serializer is used to detect the format
- THEN the JSON format SHALL be selected

### Requirement: MessagePack Format
The system SHALL provide a MessagePack serializer identified by name `"msgpack"` and code `0x02`. MessagePack deserialization SHALL strictly require the magic byte and SHALL NOT accept legacy data.

#### Scenario: MessagePack format code
- GIVEN the MessagePack serializer
- WHEN its code is accessed
- THEN the value `0x02` SHALL be returned

#### Scenario: MessagePack instant precision
- GIVEN a MessagePack serializer and an event carrying an instant
- WHEN the event is serialized and deserialized back
- THEN the instant SHALL be restored with nanosecond precision

#### Scenario: Wrong magic byte rejected
- GIVEN MessagePack data preceded by an unexpected magic byte
- WHEN deserialization is attempted
- THEN a serialization exception SHALL be thrown

### Requirement: Serializer Factory
The system SHALL provide a serializer factory that registers serializers by name and code, resolves serializers by name, by code, and by inspecting data, and SHALL register the JSON and MessagePack serializers by default.

#### Scenario: Default serializers registered
- GIVEN a factory created with defaults
- WHEN the registered names are inspected
- THEN both `"json"` and `"msgpack"` SHALL be present

#### Scenario: Resolve by name
- GIVEN a factory with a JSON serializer registered
- WHEN a serializer is resolved by name `"json"`
- THEN the JSON serializer SHALL be returned

#### Scenario: Unknown name rejected
- GIVEN a factory without a serializer for a given name
- WHEN a serializer is resolved by that name
- THEN a serialization exception SHALL be thrown

#### Scenario: Detect format from data
- GIVEN bytes produced by the MessagePack serializer
- WHEN the serializer is resolved by inspecting the data
- THEN the MessagePack serializer SHALL be returned

### Requirement: Deserialization Security Whitelist
The system SHALL restrict polymorphic deserialization to an explicit whitelist of allowed classes and packages. The default whitelist SHALL include the framework package `io.github.vovten.eventflow`. Classes outside the whitelist SHALL be rejected during deserialization.

#### Scenario: Allowed class accepted
- GIVEN a class within an allowed package
- WHEN the whitelist is queried for that class
- THEN the class SHALL be accepted

#### Scenario: Unknown class rejected
- GIVEN a class outside every allowed package and not explicitly allowed
- WHEN the whitelist is queried for that class
- THEN the class SHALL be rejected

#### Scenario: Allow additional package
- GIVEN a package added via the allow mechanism
- WHEN the whitelist is queried for a class in that package
- THEN the class SHALL be accepted

#### Scenario: Malicious payload rejected
- GIVEN serialized data whose embedded type name is not allowed
- WHEN the data is deserialized
- THEN deserialization SHALL be denied
- AND a serialization exception SHALL be thrown

### Requirement: Custom Serializers
The system SHALL support registering custom serializers in the factory. A custom serializer SHALL define its own name and format code.

#### Scenario: Register custom serializer
- GIVEN a custom serializer
- WHEN it is registered in the factory
- THEN it SHALL be resolvable by its name and code

#### Scenario: Null serializer rejected
- GIVEN a null serializer
- WHEN it is registered in the factory
- THEN an `IllegalArgumentException` SHALL be thrown
