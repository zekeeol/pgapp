CREATE TABLE IF NOT EXISTS pgapp_clients (
  id BIGSERIAL PRIMARY KEY,
  client_key TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  secret_hash TEXT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT true,
  roles JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pgapp_clients_key_hash
  ON pgapp_clients(key_hash);

CREATE INDEX IF NOT EXISTS idx_pgapp_clients_active
  ON pgapp_clients(active)
  WHERE active = true;
