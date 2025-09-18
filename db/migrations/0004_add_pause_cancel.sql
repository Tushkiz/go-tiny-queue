-- +goose Up
ALTER TABLE tasks
  ADD COLUMN paused_until DATETIME(6) NULL,
  ADD COLUMN canceled_at DATETIME(6) NULL;

CREATE INDEX idx_tasks_paused_until ON tasks (paused_until);
CREATE INDEX idx_tasks_canceled_at ON tasks (canceled_at);

-- +goose Down
ALTER TABLE tasks DROP INDEX IF EXISTS idx_tasks_paused_until;
ALTER TABLE tasks DROP INDEX IF EXISTS idx_tasks_canceled_at;
ALTER TABLE tasks DROP COLUMN IF EXISTS paused_until;
ALTER TABLE tasks DROP COLUMN IF EXISTS canceled_at;
