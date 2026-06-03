# cache-service Delta Specification

## Purpose
Extend the Cache service specification with atomic operations: Increment, Decrement, SetNX, GetSet, Append, and Prepend.

## ADDED Requirements

### Requirement: Atomic numeric operations
The Cache service SHALL provide atomic `Increment` and `Decrement` RPCs that operate on numeric cache values with row-level concurrency control. The service SHALL parse stored values as signed 64-bit integers, apply the requested delta, and return the new value.

#### Scenario: Concurrent increments are serialized
- **WHEN** two clients concurrently call `Increment` on the same key
- **THEN** each increment SHALL be applied atomically and the final value SHALL equal the sum of both deltas

#### Scenario: Increment on non-numeric value fails
- **WHEN** a client calls `Increment` on a key storing non-numeric bytes
- **THEN** the service SHALL return an invalid-argument error

### Requirement: Atomic conditional and exchange operations
The Cache service SHALL provide atomic `SetNX` (set-if-not-exists) and `GetSet` (get-and-set) RPCs. `SetNX` SHALL create the key only if it does not exist and return the creation status. `GetSet` SHALL atomically return the current value and set a new value.

#### Scenario: SetNX succeeds on non-existent key
- **WHEN** a client calls `SetNX` on a key that does not exist
- **THEN** the service SHALL create the key and return true, with no other concurrent writer able to create the same key

#### Scenario: GetSet returns old value
- **WHEN** a client calls `GetSet` on an existing key with a new value
- **THEN** the service SHALL return the old value and atomically store the new value

### Requirement: Atomic byte concatenation operations
The Cache service SHALL provide atomic `Append` and `Prepend` RPCs that concatenate bytes to the end or beginning of an existing value. Both operations SHALL create the key with the provided bytes if it does not exist and SHALL return the new total byte length.

#### Scenario: Append to existing value
- **WHEN** a client appends bytes to an existing key
- **THEN** the service SHALL return the new total length and subsequent gets SHALL return the concatenated value

#### Scenario: Prepend creates non-existent key
- **WHEN** a client prepends bytes to a non-existent key
- **THEN** the service SHALL create the key with the provided bytes and return the byte length
