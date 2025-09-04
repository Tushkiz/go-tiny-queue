package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Store struct {
	DB *gorm.DB
}

func NewStore(dsn string) (*Store, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
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
		NextRunAt:      time.Now(),
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
		Payload:        payloadBytes,
		Status:         StatusPending,
		Priority:       cfg.Priority,
		Attempt:        0,
		MaxAttempts:    cfg.MaxAttempts,
		LastError:      nil,
		IdempotencyKey: cfg.IdempotencyKey,
		LeaseExpiresAt: nil,
		NextRunAt:      cfg.NextRunAt,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
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
