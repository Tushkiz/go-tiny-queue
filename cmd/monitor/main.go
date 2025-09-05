package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/tushkiz/go-tiny-queue/internal/queue"
	"github.com/tushkiz/go-tiny-queue/internal/util"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dsn := util.Getenv("DB_DSN", "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC")
	store, err := queue.NewStore(dsn)
	if err != nil {
		log.Fatalf("monitor: store init error: %v", err)
	}

	fmt.Println("monitor: starting (Ctrl-C to exit)")
	runTUI(ctx, store)
	fmt.Println("monitor: stopped")
}

// runTUI renders a very simple screen that shows metrics snapshot and refreshes.
func runTUI(ctx context.Context, store *queue.Store) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	staleAfterStr := util.Getenv("WORKER_STALE_AFTER", "60s")
	staleAfter, _ := time.ParseDuration(staleAfterStr)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Clear screen (ANSI) and redraw
			fmt.Print("\033[2J\033[H")
			fmt.Println("Tiny Queue - Metrics (DB Snapshot)")
			fmt.Println(time.Now().UTC().Format(time.RFC3339))
			leased, completed, failed, rescheduled, pending, dead, err := readSnapshot(ctx, store)
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println()
			fmt.Printf("Leased (in_flight) : %d\n", leased)
			fmt.Printf("Completed          : %d\n", completed)
			fmt.Printf("Failed*            : %d\n", failed)
			fmt.Printf("Rescheduled*       : %d\n", rescheduled)
			fmt.Println()
			fmt.Printf("Pending            : %d\n", pending)
			fmt.Printf("Dead (DLQ)         : %d\n", dead)
			fmt.Println()
			fmt.Println("* Failed: pending with last_error; Rescheduled: pending with attempt>0")
			fmt.Println()

			// Workers section
			ws, err := listWorkers(ctx, store)
			if err != nil {
				fmt.Println("workers error:", err)
			} else {
				fmt.Println("Workers (last_seen_at UTC):")
				if len(ws) == 0 {
					fmt.Println("  (none)")
				} else {
					now := time.Now().UTC()
					for _, w := range ws {
						age := now.Sub(w.LastSeenAt)
						status := "ok"
						if age > staleAfter {
							status = "STALE"
						}
						fmt.Printf("  %-36s  %s  (age %s)  [%s]\n", w.ID, w.LastSeenAt.UTC().Format(time.RFC3339Nano), age.Truncate(time.Millisecond), status)
					}
				}
			}

			fmt.Println()
			fmt.Println("Press Ctrl-C to exit")
		}
	}
}

func readSnapshot(ctx context.Context, store *queue.Store) (leased, completed, failed, rescheduled, pending, dead int64, err error) {
	// leased: status=in_flight
	if leased, err = countWhere(ctx, store, "status = ?", queue.StatusInFlight); err != nil {
		return
	}
	if completed, err = countWhere(ctx, store, "status = ?", queue.StatusCompleted); err != nil {
		return
	}
	// failed: pending with last_error not null (recent failures waiting to retry)
	if failed, err = countWhere(ctx, store, "status = ? AND last_error IS NOT NULL", queue.StatusPending); err != nil {
		return
	}
	// rescheduled: pending with attempt > 0
	if rescheduled, err = countWhere(ctx, store, "status = ? AND attempt > 0", queue.StatusPending); err != nil {
		return
	}
	if pending, err = countWhere(ctx, store, "status = ?", queue.StatusPending); err != nil {
		return
	}
	if dead, err = countWhere(ctx, store, "status = ?", queue.StatusDead); err != nil {
		return
	}
	return
}

func countWhere(ctx context.Context, store *queue.Store, where string, args ...any) (int64, error) {
	var n int64
	err := store.DB.WithContext(ctx).Model(&queue.Task{}).Where(where, args...).Count(&n).Error
	return n, err
}

type workerRow struct {
	ID         string
	LastSeenAt time.Time
}

func listWorkers(ctx context.Context, store *queue.Store) ([]workerRow, error) {
	var rows []workerRow
	if err := store.DB.
		WithContext(ctx).
		Table("workers").
		Select("id, last_seen_at").
		Order("last_seen_at DESC").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}
