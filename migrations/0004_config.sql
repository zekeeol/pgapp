CREATE TABLE IF NOT EXISTS config_scopes (
  id BIGSERIAL PRIMARY KEY,
  app_id TEXT NOT NULL,
  environment TEXT NOT NULL,
  cluster TEXT NOT NULL,
  namespace TEXT NOT NULL,
  current_revision BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (app_id, environment, cluster, namespace)
);

CREATE TABLE IF NOT EXISTS config_items (
  id BIGSERIAL PRIMARY KEY,
  scope_id BIGINT NOT NULL REFERENCES config_scopes(id) ON DELETE CASCADE,
  config_key TEXT NOT NULL,
  value_json JSONB NOT NULL,
  deleted BOOLEAN NOT NULL DEFAULT false,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (scope_id, config_key)
);

CREATE TABLE IF NOT EXISTS config_releases (
  id BIGSERIAL PRIMARY KEY,
  scope_id BIGINT NOT NULL REFERENCES config_scopes(id) ON DELETE CASCADE,
  revision BIGINT NOT NULL,
  snapshot_json JSONB NOT NULL,
  checksum TEXT NOT NULL,
  message TEXT NOT NULL DEFAULT '',
  published_by TEXT NOT NULL DEFAULT '',
  published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (scope_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_config_scopes_lookup
  ON config_scopes(app_id, environment, cluster, namespace);

CREATE INDEX IF NOT EXISTS idx_config_items_scope_key
  ON config_items(scope_id, config_key);

CREATE INDEX IF NOT EXISTS idx_config_releases_scope_revision_desc
  ON config_releases(scope_id, revision DESC);
