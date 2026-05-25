CREATE TABLE IF NOT EXISTS cache_namespaces (
  name TEXT PRIMARY KEY,
  generation BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cache_entries (
  id BIGSERIAL PRIMARY KEY,
  namespace TEXT NOT NULL,
  generation BIGINT NOT NULL,
  key_hash TEXT NOT NULL,
  cache_key TEXT NOT NULL,
  value_bytes BYTEA NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  expires_at TIMESTAMPTZ,
  size_bytes BIGINT NOT NULL,
  last_accessed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  access_count BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (namespace, generation, key_hash)
);

CREATE INDEX IF NOT EXISTS idx_cache_entries_namespace_generation
  ON cache_entries(namespace, generation);

CREATE INDEX IF NOT EXISTS idx_cache_entries_expiry
  ON cache_entries(namespace, generation, expires_at);

CREATE INDEX IF NOT EXISTS idx_cache_entries_lru
  ON cache_entries(namespace, generation, last_accessed_at);

CREATE TABLE IF NOT EXISTS cache_stats (
  singleton BOOLEAN PRIMARY KEY DEFAULT true,
  hits BIGINT NOT NULL DEFAULT 0,
  misses BIGINT NOT NULL DEFAULT 0,
  writes BIGINT NOT NULL DEFAULT 0,
  deletes BIGINT NOT NULL DEFAULT 0,
  evictions BIGINT NOT NULL DEFAULT 0,
  expired_removals BIGINT NOT NULL DEFAULT 0,
  CHECK (singleton)
);

INSERT INTO cache_stats(singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;
