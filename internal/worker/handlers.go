package worker

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// Handler implements the work for a given task type
type Handler func(ctx context.Context, payload []byte) error

// Registry maps task types to handlers
type Registry map[string]Handler

// Example handlers

func NoopHandler(ctx context.Context, payload []byte) error {
	return nil
}

func SlowMaybeFailHandler(ctx context.Context, payload []byte) error {
	time.Sleep(5 * time.Second)
	if rand.Intn(2) == 0 {
		return nil
	}
	return errors.New("failed")
}

func ErrHandler(ctx context.Context, payload []byte) error {
	return errors.New("handler error: forced failure")
}

func DefaultRegistry() Registry {
	return Registry{
		"send_test_email": SlowMaybeFailHandler,
	}
}
