package main

import (
	"context"
	"encoding/json"
	"flag"
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

	// Flags: -type <string> -payload '<json>'
	var (
		taskType   = flag.String("type", "", "task type (required)")
		payloadStr = flag.String("payload", "", "payload as JSON string (required)")
	)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s -type <type> -payload '<json>'\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if *taskType == "" || *payloadStr == "" {
		flag.Usage()
		os.Exit(2)
	}

	// Parse payload JSON
	var payload any
	if err := json.Unmarshal([]byte(*payloadStr), &payload); err != nil {
		fmt.Fprintf(os.Stderr, "invalid payload JSON: %v\n", err)
		os.Exit(2)
	}

	t, err := store.Enqueue(
		ctx,
		*taskType,
		payload,
		queue.WithPriority(10),
		queue.WithMaxAttempts(5),
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
