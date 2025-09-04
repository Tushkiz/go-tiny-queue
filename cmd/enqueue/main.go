package main

import (
	"context"
	"fmt"
	"os"

	"github.com/tushkiz/go-tiny-queue/internal/queue"
)

func main() {
	ctx := context.Background()
	dsn := getenv("DB_DSN", "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC")
	store, err := queue.NewStore(dsn)
	if err != nil {
		panic(err)
	}

	// Example payload
	email := "test@example.com"
	taskType := "send_test_email"
	payload := map[string]any{
		"email":   email,
		"subject": "Testing queue",
	}

	idempotencyKey := email + "__" + taskType

	t, err := store.Enqueue(
		ctx,
		taskType,
		payload,
		queue.WithPriority(10),
		queue.WithMaxAttempts(5),
		queue.WithIdempotencyKey(idempotencyKey),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf(
		"enqueued task:\n"+
			"  id          = %s\n"+
			"  type        = %s\n"+
			"  status      = %s\n"+
			"  next_run_at = %s\n",
		t.ID,
		t.Type,
		t.Status,
		t.NextRunAt,
	)
}

func getenv(key string, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
