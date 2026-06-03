# cache-atomic-operations Specification

## Purpose
TBD - created by archiving change add-phase-two-features. Update Purpose after archive.
## Requirements
### Requirement: Atomic Increment and Decrement
The Cache service SHALL support atomic `Increment` and `Decrement` operations on cache entries storing numeric values. The operations SHALL parse the stored value as a signed 64-bit integer, add or subtract the provided delta, store the result, and return the new value. If the key does not exist, `Increment` SHALL create it with the delta as the initial value. `Decrement` on a non-existent key SHALL create it with the negated delta as the initial value. Operations on non-numeric values SHALL return an invalid-argument error.

#### Scenario: Increment existing numeric value
- **WHEN** key `counter` stores value `10` and a client calls `Increment` with delta `5`
- **THEN** the service SHALL atomically update the value to `15`, return `15`, and the operation SHALL be isolated from concurrent modifications

#### Scenario: Increment creates non-existent key
- **WHEN** key `counter` does not exist and a client calls `Increment` with delta `1`
- **THEN** the service SHALL create the key with value `1` and return `1`

#### Scenario: Decrement existing numeric value
- **WHEN** key `counter` stores value `10` and a client calls `Decrement` with delta `3`
- **THEN** the service SHALL atomically update the value to `7` and return `7`

#### Scenario: Increment on non-numeric value fails
- **WHEN** key `data` stores a non-numeric byte value and a client calls `Increment`
- **THEN** the service SHALL return an invalid-argument error indicating the value is not numeric

### Requirement: Atomic SetNX (Set-if-Not-eXists)
The Cache service SHALL support atomic `SetNX` that sets a key to a value only if the key does not already exist in the current namespace generation. The operation SHALL return true if the key was created and false if it already existed. The operation SHALL be atomic with respect to other writes to the same key.

#### Scenario: SetNX creates a new key
- **WHEN** key `lock:resource-1` does not exist and a client calls `SetNX`
- **THEN** the service SHALL create the key, return true, and no concurrent `SetNX` for the same key SHALL also return true

#### Scenario: SetNX fails on existing key
- **WHEN** key `lock:resource-1` already exists and a client calls `SetNX`
- **THEN** the service SHALL return false and the existing value SHALL remain unchanged

### Requirement: Atomic GetSet (get-and-set)
The Cache service SHALL support atomic `GetSet` that atomically retrieves the current value of a key and sets it to a new value. If the key does not exist, the operation SHALL return null/miss and set the key to the new value. The get and set SHALL occur within a single atomic operation isolated from concurrent modifications.

#### Scenario: GetSet on existing key
- **WHEN** key `last-update` stores value `v1` and a client calls `GetSet` with new value `v2`
- **THEN** the service SHALL return `v1` and atomically update the key to `v2`

#### Scenario: GetSet on non-existent key
- **WHEN** key `last-update` does not exist and a client calls `GetSet` with value `v1`
- **THEN** the service SHALL return a miss indicator and create the key with value `v1`

### Requirement: Atomic Append and Prepend
The Cache service SHALL support atomic `Append` and `Prepend` operations that concatenate the provided bytes to the end or beginning of an existing value. If the key does not exist, both operations SHALL create it with the provided bytes as the initial value. The operations SHALL return the new total byte length.

#### Scenario: Append to existing bytes
- **WHEN** key `log` stores bytes `[0x01, 0x02]` and a client calls `Append` with bytes `[0x03, 0x04]`
- **THEN** the service SHALL atomically update the value to `[0x01, 0x02, 0x03, 0x04]` and return `4`

#### Scenario: Prepend to existing bytes
- **WHEN** key `log` stores bytes `[0x03, 0x04]` and a client calls `Prepend` with bytes `[0x01, 0x02]`
- **THEN** the service SHALL atomically update the value to `[0x01, 0x02, 0x03, 0x04]` and return `4`

#### Scenario: Append creates non-existent key
- **WHEN** key `log` does not exist and a client calls `Append` with bytes `[0x01, 0x02]`
- **THEN** the service SHALL create the key with value `[0x01, 0x02]` and return `2`

### Requirement: TTL and namespace isolation for atomic operations
All atomic operations SHALL respect existing Cache semantics: entries with expired TTL SHALL be treated as non-existent, namespace invalidation SHALL prevent operations on old-generation entries, and capacity enforcement SHALL apply after atomic writes.

#### Scenario: Increment ignores expired entry
- **WHEN** key `counter` has expired and a client calls `Increment`
- **THEN** the service SHALL create a new entry with the delta as the initial value

#### Scenario: SetNX on invalidated namespace
- **WHEN** namespace `N` has been invalidated and a client calls `SetNX` for a key that existed before invalidation
- **THEN** the service SHALL create the key (old generation entries are invisible)

