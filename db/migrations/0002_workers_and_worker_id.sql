-- 0002: workers table and worker_id on tasks

-- +goose Up
-- Create workers table
CREATE TABLE IF NOT EXISTS workers (
  id VARCHAR(36) NOT NULL PRIMARY KEY,
  last_seen_at DATETIME(6) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Add worker_id column to tasks (nullable) and index
ALTER TABLE tasks
  ADD COLUMN worker_id VARCHAR(36) NULL;

ALTER TABLE tasks
  ADD INDEX idx_tasks_worker_id (worker_id);

-- +goose Down
-- Reverse workers table and worker_id additions
ALTER TABLE tasks DROP INDEX IF EXISTS idx_tasks_worker_id;
ALTER TABLE tasks DROP COLUMN IF EXISTS worker_id;
DROP TABLE IF EXISTS workers;