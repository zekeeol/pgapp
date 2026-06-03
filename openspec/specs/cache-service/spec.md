# cache-service Specification

## Purpose
Define the PostgreSQL-backed Cache service behavior for namespace-scoped
key/value storage, TTL expiration, invalidation, capacity enforcement, and
statistics.
## Requirements
### Requirement: Namespace-scoped key/value operations
The Cache service MUST store opaque byte values by namespace and key. It MUST support single-key and batch variants for setting, getting, deleting, and checking entries.

#### Scenario: Store and retrieve a value
- **WHEN** a client stores byte value `V` for key `K` in namespace `N`
- **THEN** a subsequent get for namespace `N` and key `K` MUST return byte value `V`

#### Scenario: Isolate namespaces
- **WHEN** the same key `K` is stored in namespaces `A` and `B`
- **THEN** reads and deletes in namespace `A` MUST NOT affect the value in namespace `B`

#### Scenario: Batch get preserves per-key results
- **WHEN** a client requests multiple keys and only some keys exist
- **THEN** the response MUST identify hits and misses for each requested key

### Requirement: TTL expiration
The Cache service MUST support per-entry TTL. Expired entries MUST be treated as missing on reads even when physical cleanup has not removed their rows.

#### Scenario: Entry expires before read
- **WHEN** a key is written with a TTL and the current time is after its expiration time
- **THEN** a get for that key MUST return a miss

#### Scenario: Entry without TTL remains readable
- **WHEN** a key is written without a TTL and no default expiration applies
- **THEN** a get for that key MUST return the stored value until it is deleted, invalidated, or evicted

### Requirement: Invalidation and logical capacity
The Cache service MUST support exact key deletion and namespace invalidation. It MUST enforce configured logical capacity limits for maximum keys and maximum value bytes. When capacity is exceeded, it MUST evict expired entries before live entries, then evict live entries using least-recently-used order.

#### Scenario: Delete hides one key
- **WHEN** a client deletes key `K` in namespace `N`
- **THEN** subsequent reads for `N/K` MUST return a miss

#### Scenario: Namespace invalidation hides old entries
- **WHEN** a client invalidates namespace `N`
- **THEN** entries written to namespace `N` before the invalidation MUST return misses

#### Scenario: Capacity eviction prefers expired entries
- **WHEN** a write would exceed the configured logical capacity and expired entries exist
- **THEN** the service MUST evict expired entries before evicting non-expired entries

#### Scenario: Capacity eviction falls back to least recently used
- **WHEN** a write would exceed the configured logical capacity and no expired entries are available
- **THEN** the service MUST evict live entries in least-recently-used order until the write can fit or report that the write cannot fit

### Requirement: Cache statistics
The Cache service MUST expose cache statistics including hits, misses, writes, deletes, evictions, expired removals, logical key count, logical byte size, and per-namespace usage.

#### Scenario: Hit and miss counters update
- **WHEN** one get request returns a value and another get request misses
- **THEN** cache statistics MUST report one additional hit and one additional miss

#### Scenario: Eviction counters update
- **WHEN** the service evicts entries to enforce capacity
- **THEN** cache statistics MUST report the number of evicted entries

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
