ALTER TABLE config_scopes
  ADD COLUMN IF NOT EXISTS json_schema JSONB;
