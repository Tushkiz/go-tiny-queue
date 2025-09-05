package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/tushkiz/go-tiny-queue/internal/queue"
	"github.com/tushkiz/go-tiny-queue/internal/util"
)

func main() {
	ctx := context.Background()

	olderThanStr := flag.String("older-than", "168h", "Delete workers older than this duration and not referenced by any task (e.g., 24h, 168h)")
	flag.Parse()

	olderThan, err := time.ParseDuration(*olderThanStr)
	if err != nil {
		panic(fmt.Errorf("invalid -older-than: %w", err))
	}

	dsn := util.Getenv("DB_DSN", "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC")
	store, err := queue.NewStore(dsn)
	if err != nil {
		panic(err)
	}

	cutoff := time.Now().UTC().Add(-olderThan)
	res := store.DB.WithContext(ctx).Exec(`
		DELETE FROM workers w
		WHERE w.last_seen_at < ?
		  AND NOT EXISTS (
		    SELECT 1 FROM tasks t WHERE t.worker_id = w.id
		  )
	`, cutoff)
	if res.Error != nil {
		panic(res.Error)
	}

	fmt.Printf("purge-workers: deleted %d stale unreferenced worker(s)\n", res.RowsAffected)
}
