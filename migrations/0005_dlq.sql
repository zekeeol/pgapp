ALTER TABLE mq_queues
  ADD COLUMN IF NOT EXISTS max_redelivery_count INTEGER DEFAULT NULL;

CREATE TABLE IF NOT EXISTS mq_dlq (
  id BIGSERIAL PRIMARY KEY,
  queue_id BIGINT NOT NULL REFERENCES mq_queues(id) ON DELETE CASCADE,
  original_message_id BIGINT NOT NULL,
  payload JSONB NOT NULL,
  headers JSONB NOT NULL DEFAULT '{}'::jsonb,
  read_count INTEGER NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL,
  dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reason TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_mq_dlq_queue_dead_lettered
  ON mq_dlq(queue_id, dead_lettered_at DESC, id DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mq_dlq_queue_original_message
  ON mq_dlq(queue_id, original_message_id);
