CREATE TABLE IF NOT EXISTS admin_log_events (
  id BIGSERIAL PRIMARY KEY,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  level TEXT NOT NULL,
  target TEXT NOT NULL,
  message TEXT NOT NULL,
  request_id TEXT,
  fields_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_admin_log_events_occurred_at
  ON admin_log_events(occurred_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_admin_log_events_level
  ON admin_log_events(level);

CREATE INDEX IF NOT EXISTS idx_admin_log_events_target
  ON admin_log_events(target);
