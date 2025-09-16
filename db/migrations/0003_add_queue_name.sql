-- +goose Up
ALTER TABLE tasks
  ADD COLUMN queue_name VARCHAR(64) NOT NULL DEFAULT 'default';

CREATE INDEX idx_tasks_queue_name ON tasks (queue_name);

-- +goose Down
ALTER TABLE tasks DROP INDEX IF EXISTS idx_tasks_queue_name;
ALTER TABLE tasks DROP COLUMN IF EXISTS queue_name;
