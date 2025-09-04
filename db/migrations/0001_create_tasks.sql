-- +goose Up
CREATE TABLE IF NOT EXISTS tasks (
	id CHAR(36) NOT NULL PRIMARY KEY,
	type VARCHAR(255) NOT NULL,
    payload JSON NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    last_error TEXT NULL,
    idempotency_key VARCHAR(128) NULL,
    lease_expires_at DATETIME(6) NULL,
    next_run_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT tasks_idempotency_key_unique UNIQUE (idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_tasks_status ON tasks (status);
CREATE INDEX idx_tasks_next_run_at ON tasks (next_run_at);
CREATE INDEX idx_tasks_priority ON tasks (priority);
CREATE INDEX idx_tasks_lease_expires_at ON tasks (lease_expires_at);

-- +goose Down
DROP TABLE IF EXISTS tasks;

    
	