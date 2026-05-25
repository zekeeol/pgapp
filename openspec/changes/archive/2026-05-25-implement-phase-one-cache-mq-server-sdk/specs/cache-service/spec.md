## ADDED Requirements

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
