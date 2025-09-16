package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type Store struct {
	DB *gorm.DB
}

func NewStore(dsn string) (*Store, error) {
	// Configure a quiet logger that ignores record-not-found and only logs errors.
	gormLogger := logger.New(
		log.New(os.Stdout, "", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  logger.Error,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: gormLogger})
	if err != nil {
		return nil, err
	}

	return &Store{DB: db}, nil
}

// Enqueue inserts a new task. If idempotencyKey is provided and already exists,
// the existing task is returned (no duplicate row).
func (s *Store) Enqueue(ctx context.Context, taskType string, payload any, opts ...EnqueueOption) (*Task, error) {
	cfg := enqueueConfig{
		Priority:       0,
		MaxAttempts:    5,
		NextRunAt:      time.Now().UTC(),
		QueueName:      "default",
		IdempotencyKey: nil,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Serialize payload to JSON
	var payloadBytes []byte
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		payloadBytes = b
	} else {
		payloadBytes = []byte("null")
	}

	// if indempotency key exists, try insert with DoNothing then fetch existing.
	id := uuid.NewString()
	t := &Task{
		ID:             id,
		Type:           taskType,
		QueueName:      cfg.QueueName,
		Payload:        payloadBytes,
		Status:         StatusPending,
		Priority:       cfg.Priority,
		Attempt:        0,
		MaxAttempts:    cfg.MaxAttempts,
		LastError:      nil,
		IdempotencyKey: cfg.IdempotencyKey,
		LeaseExpiresAt: nil,
		NextRunAt:      cfg.NextRunAt,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	// Insert with ON CONFLICT DO NOTHING
	res := s.DB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "idempotency_key"}},
		DoNothing: true,
	}).Create(t)

	if res.Error != nil {
		return nil, res.Error
	}

	if res.RowsAffected == 1 {
		// New row inserted
		return t, nil
	}

	// No row inserted, fetch and return existing row
	if cfg.IdempotencyKey == nil {
		return nil, errors.New("enqueue: no row inserted")
	}

	var existing Task
	if err := s.DB.WithContext(ctx).Where("idempotency_key = ?", *cfg.IdempotencyKey).First(&existing).Error; err != nil {
		return nil, err
	}

	return &existing, nil
}

// Options for Enqueue

type enqueueConfig struct {
	Priority       int
	MaxAttempts    int
	NextRunAt      time.Time
	QueueName      string
	IdempotencyKey *string
}

type EnqueueOption func(*enqueueConfig)

func WithPriority(p int) EnqueueOption {
	return func(ec *enqueueConfig) { ec.Priority = p }
}

func WithMaxAttempts(n int) EnqueueOption {
	return func(ec *enqueueConfig) { ec.MaxAttempts = n }
}

func WithIdempotencyKey(k string) EnqueueOption {
	return func(ec *enqueueConfig) { ec.IdempotencyKey = &k }
}

func WithQueueName(name string) EnqueueOption {
	return func(ec *enqueueConfig) { ec.QueueName = name }
}

// FetchAndLease finds one due task and leases it by setting status=in_flight
// attempt=attempt+1, lease_expires_at = now + visibilityTimeout.
func (s *Store) FetchAndLease(ctx context.Context, workerID string, visibilityTimeout time.Duration) (*Task, error) {
	tx := s.DB.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()

	var t Task
	// Lock due, unleased or expired lease tasks

	err := tx.
		Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where("status = ? AND next_run_at <= NOW(6) AND (lease_expires_at IS NULL OR lease_expires_at <= NOW(6))", StatusPending).
		Order("priority DESC, next_run_at ASC, id ASC").
		Limit(1).
		Take(&t).Error

	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, sql.ErrNoRows
		}
		return nil, err
	}

	newLease := time.Now().UTC().Add(visibilityTimeout)
	now := time.Now().UTC()
	// Update the selected row to mark as in-flight and set lease
	if err := tx.Model(&Task{}).Where("id = ?", t.ID).Updates(map[string]any{
		"status":           StatusInFlight,
		"attempt":          gorm.Expr("attempt + 1"),
		"lease_expires_at": newLease,
		"worker_id":        workerID,
		"updated_at":       now,
	}).Error; err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	// Reflect changes in returned struct
	t.Status = StatusInFlight
	t.Attempt += 1
	t.LeaseExpiresAt = &newLease
	t.UpdatedAt = now
	t.WorkerID = &workerID

	return &t, nil
}

// FetchAndLeaseFromQueues is like FetchAndLease but restricts selection to the given queue names.
func (s *Store) FetchAndLeaseFromQueues(ctx context.Context, workerID string, visibilityTimeout time.Duration, queues []string) (*Task, error) {
	tx := s.DB.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()

	var t Task
	// Lock due, unleased or expired lease tasks limited to the specified queues
	err := tx.
		Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where("status = ? AND next_run_at <= NOW(6) AND (lease_expires_at IS NULL OR lease_expires_at <= NOW(6)) AND queue_name IN ?", StatusPending, queues).
		Order("priority DESC, next_run_at ASC, id ASC").
		Limit(1).
		Take(&t).Error

	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, sql.ErrNoRows
		}
		return nil, err
	}

	newLease := time.Now().UTC().Add(visibilityTimeout)
	now := time.Now().UTC()
	// Update the selected row to mark as in-flight and set lease
	if err := tx.Model(&Task{}).Where("id = ?", t.ID).Updates(map[string]any{
		"status":           StatusInFlight,
		"attempt":          gorm.Expr("attempt + 1"),
		"lease_expires_at": newLease,
		"worker_id":        workerID,
		"updated_at":       now,
	}).Error; err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	// Reflect changes in returned struct
	t.Status = StatusInFlight
	t.Attempt += 1
	t.LeaseExpiresAt = &newLease
	t.UpdatedAt = now
	t.WorkerID = &workerID

	return &t, nil
}

// CompleteTask marks a task as completed and clears the lease
func (s *Store) CompleteTask(ctx context.Context, id string) error {
	now := time.Now().UTC()
	return s.DB.WithContext(ctx).Model(&Task{}).Where("id = ?", id).Updates(map[string]any{
		"status":           StatusCompleted,
		"lease_expires_at": nil,
		"worker_id":        nil,
		"updated_at":       now,
	}).Error
}

// ExtendLease attempts to extend lease if the current lease matches
// Returns true if extended, false if not
func (s *Store) ExtendLease(ctx context.Context, id string, currentLease time.Time, extendBy time.Duration) (bool, error) {
	newLease := time.Now().UTC().Add(extendBy)

	res := s.DB.WithContext(ctx).Model(&Task{}).Where("id = ? AND status = ? AND lease_expires_at = ?", id, StatusInFlight, currentLease).Updates(map[string]any{
		"lease_expires_at": newLease,
		"updated_at":       time.Now(),
	})

	if res.Error != nil {
		return false, res.Error
	}

	return res.RowsAffected == 1, nil
}

// FailAndReschedule sets status back to pending with an increased next_run_at using
// expenential backoff and optional jitter. If attempts >= max_attempts after increment,
// the task is marked dead and not rescheduled
func (s *Store) FailAndReschedule(ctx context.Context, id string, baseBackoff time.Duration, errMsg string) (dead bool, err error) {
	tx := s.DB.WithContext(ctx).Begin()
	if tx.Error != nil {
		return false, tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()

	var t Task

	// fetch with UPDATE lock to avoid races
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&t).Error; err != nil {
		_ = tx.Rollback()

		return false, err
	}

	now := time.Now().UTC()
	// attempt was already incremented when leased, we inspect it now to compute backoff
	nextAttempt := t.Attempt
	if nextAttempt >= t.MaxAttempts {
		// Move to DLQ, mark as dead and do not reschedule
		if err := tx.Model(&Task{}).Where("id = ?", id).Updates(map[string]any{
			"status":           StatusDead,
			"last_error":       errMsg,
			"lease_expires_at": nil,
			"worker_id":        nil,
			"updated_at":       now,
		}).Error; err != nil {
			_ = tx.Rollback()
			return false, err
		}
		if err := tx.Commit().Error; err != nil {
			return false, err
		}
		return true, nil
	}

	// Compute backoff: base * 2^(attempt - 1) with small jitter
	delay := BackoffWithJitter(baseBackoff, nextAttempt)

	if err := tx.Model(&Task{}).Where("id = ?", id).Updates(map[string]any{
		"status":           StatusPending,
		"last_error":       errMsg,
		"lease_expires_at": nil,
		"worker_id":        nil,
		"next_run_at":      now.Add(delay).UTC(),
		"updated_at":       now,
	}).Error; err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if err := tx.Commit().Error; err != nil {
		return false, err
	}
	return false, nil
}

// BackoffWithJitter computes exponential backoff based on the attempt number.
// attempt is 1-based
func BackoffWithJitter(base time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	// base * 2^(attempt - 1)
	delay := base << (attempt - 1)
	// apply jitter +/-10%
	jitterFrac := 0.10
	nowNs := time.Now().UTC().UnixNano()
	// pseudo-random but deterministic-enough without math/rand seeding
	sign := int64(1)
	if nowNs&1 == 0 {
		sign = -1
	}
	jitter := time.Duration(float64(delay) * jitterFrac)
	return delay + time.Duration(sign)*jitter/2
}

// ListTasks returns tasks filtered by status ("", "all" for no filter) up to the given limit.
func (s *Store) ListTasks(ctx context.Context, status string, limit int) ([]Task, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := s.DB.WithContext(ctx).Model(&Task{})
	if status != "" && status != "all" {
		q = q.Where("status = ?", status)
	}
	var tasks []Task
	if err := q.Order("updated_at DESC").Limit(limit).Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

// RegisterWorker registers a worker and returns its ID
func (s *Store) RegisterWorker(ctx context.Context, id string) error {
	now := time.Now().UTC()
	w := Worker{ID: id, LastSeenAt: now, CreatedAt: now, UpdatedAt: now}

	return s.DB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "id"}},
		DoUpdates: clause.Assignments(map[string]any{
			"last_seen_at": now,
			"updated_at":   now,
		}),
	}).Create(&w).Error
}

func (s *Store) HeartbeatWorker(ctx context.Context, id string) error {
	now := time.Now().UTC()
	return s.DB.WithContext(ctx).Model(&Worker{}).Where("id = ?", id).Updates(map[string]any{
		"last_seen_at": now,
		"updated_at":   now,
	}).Error
}

func (s *Store) ListWorkers(ctx context.Context, limit int) ([]Worker, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var workers []Worker
	if err := s.DB.WithContext(ctx).Order("last_seen_at DESC").Limit(limit).Find(&workers).Error; err != nil {
		return nil, err
	}
	return workers, nil
}

// ReclaimExpiredFromStaleWorkers moves tasks whose lease expired and whose workers are stale back to pending.
func (s *Store) ReclaimExpiredFromStaleWorkers(ctx context.Context, staleAfter time.Duration, limit int) (int64, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	cutoff := time.Now().UTC().Add(-staleAfter)

	// We reclaim only tasks that are assigned to a worker whose last_seen_at < cutoff
	// and whose lease has expired. We clear worker_id and set next_run_at to NOW.
	res := s.DB.WithContext(ctx).Exec(`
        UPDATE tasks t
        SET t.status = ?,
            t.lease_expires_at = NULL,
            t.worker_id = NULL,
            t.updated_at = NOW(6),
            t.next_run_at = NOW(6)
        WHERE t.id IN (
            SELECT id FROM (
                SELECT t2.id
                FROM tasks t2
                JOIN workers w ON w.id = t2.worker_id
                WHERE t2.status = ?
                  AND t2.lease_expires_at IS NOT NULL
                  AND t2.lease_expires_at <= NOW(6)
                  AND w.last_seen_at < ?
                LIMIT ?
            ) AS limited
        )
    `, StatusPending, StatusInFlight, cutoff, limit)

	return res.RowsAffected, res.Error
}
