CREATE TABLE IF NOT EXISTS mq_queues (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  durable BOOLEAN NOT NULL DEFAULT true,
  max_receive_count INTEGER NOT NULL DEFAULT 0,
  default_visibility_timeout_seconds BIGINT NOT NULL DEFAULT 30,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mq_messages (
  id BIGSERIAL PRIMARY KEY,
  queue_id BIGINT NOT NULL REFERENCES mq_queues(id) ON DELETE CASCADE,
  payload JSONB NOT NULL,
  headers JSONB NOT NULL DEFAULT '{}'::jsonb,
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  visibility_timeout_at TIMESTAMPTZ,
  ack_token TEXT,
  read_count INTEGER NOT NULL DEFAULT 0,
  last_read_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE mq_messages
  ADD COLUMN IF NOT EXISTS ack_token TEXT;

CREATE INDEX IF NOT EXISTS idx_mq_messages_visible
  ON mq_messages(queue_id, available_at, visibility_timeout_at, id);

CREATE INDEX IF NOT EXISTS idx_mq_messages_inflight
  ON mq_messages(queue_id, visibility_timeout_at)
  WHERE visibility_timeout_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mq_messages_ack
  ON mq_messages(queue_id, id, ack_token)
  WHERE ack_token IS NOT NULL;

CREATE TABLE IF NOT EXISTS mq_archives (
  id BIGSERIAL PRIMARY KEY,
  queue_id BIGINT NOT NULL REFERENCES mq_queues(id) ON DELETE CASCADE,
  original_message_id BIGINT NOT NULL,
  payload JSONB NOT NULL,
  headers JSONB NOT NULL DEFAULT '{}'::jsonb,
  read_count INTEGER NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mq_archives_queue
  ON mq_archives(queue_id, archived_at);
