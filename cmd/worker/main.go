package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"
	"github.com/tushkiz/go-tiny-queue/internal/metrics"
	"github.com/tushkiz/go-tiny-queue/internal/queue"
	"github.com/tushkiz/go-tiny-queue/internal/util"
	"github.com/tushkiz/go-tiny-queue/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dsn := util.Getenv("DB_DSN", "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC")
	visStr := util.Getenv("QUEUE_VISIBILITY_TIMEOUT", "30s")
	vis, err := time.ParseDuration(visStr)
	if err != nil {
		panic(err)
	}

	heartbeatStr := util.Getenv("WORKER_HEARTBEAT_INTERVAL", "5s")
	heartbeatEvery, err := time.ParseDuration(heartbeatStr)
	if err != nil {
		panic(err)
	}
	staleStr := util.Getenv("WORKER_STALE_AFTER", "60s")
	staleAfter, err := time.ParseDuration(staleStr)
	if err != nil {
		panic(err)
	}
	reclaimStr := util.Getenv("WORKER_RECLAIM_EVERY", "10s")
	reclaimEvery, err := time.ParseDuration(reclaimStr)
	if err != nil {
		panic(err)
	}

	store, err := queue.NewStore(dsn)
	if err != nil {
		panic(err)
	}

	stopMetrics := metrics.Every(10*time.Second, func() {
		s := metrics.Default.Snapshot()
		fmt.Printf("metrics: leased=%d completed=%d failed=%d rescheduled=%d\n",
			s.Leased, s.Completed, s.Failed, s.Rescheduled)
	})
	defer stopMetrics()

	// Register worker and start heartbeats
	workerID := uuid.NewString()
	if err := store.RegisterWorker(ctx, workerID); err != nil {
		panic(fmt.Errorf("worker: register error: %w", err))
	}
	stopHB := startHeartbeat(ctx, store, workerID, heartbeatEvery)
	defer stopHB()

	// Start reclaimer loop
	stopReclaimer := startReclaimer(ctx, store, staleAfter, reclaimEvery)
	defer stopReclaimer()

	fmt.Println("worker: starting, visibility timeout =", vis, "workerID=", workerID)

	reg := worker.DefaultRegistry()
	baseBackoff := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			fmt.Println("worker: stopping")
			return
		default:
		}

		t, err := store.FetchAndLease(ctx, workerID, vis)
		if err != nil {
			// error or no task available
			time.Sleep(1 * time.Second)
			continue
		}
		if t == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		metrics.Default.IncLeased()

		fmt.Printf(
			"worker leased task:\n"+
				"  id          = %s\n"+
				"  type        = %s\n"+
				"  attempt      = %d\n",
			t.ID,
			t.Type,
			t.Attempt,
		)

		// Start lease extender for long-running jobs
		done := make(chan struct{})
		stopped := make(chan struct{})
		go leaseExtender(ctx, store, t, vis/2, done, stopped) // extend every half timeout

		// Decode payload
		var payloadBytes []byte = t.Payload
		var handler worker.Handler
		if h, ok := reg[t.Type]; ok {
			handler = h
		} else {
			// unkown type: no handlers, reschedule with error
			close(done)
			<-stopped

			metrics.Default.IncFailed()
			dead, _ := store.FailAndReschedule(ctx, t.ID, baseBackoff, "no handlers registered for type "+t.Type)
			if !dead {
				metrics.Default.IncRescheduled()
			}

			fmt.Println("worker: no handler for type, reschedule task id=", t.ID)
			continue
		}

		// Check if json is well-formed
		var tmp any
		_ = json.Unmarshal(payloadBytes, &tmp)

		// Run handler
		err = handler(ctx, payloadBytes)

		// Stop extender before updating DB
		close(done)
		<-stopped

		if err != nil {
			// Fail and reschedule with backoff
			metrics.Default.IncFailed()
			dead, ferr := store.FailAndReschedule(ctx, t.ID, baseBackoff, err.Error())
			if ferr != nil {
				fmt.Println("worker: fail/reschedule error: ", ferr)
				continue
			}
			if dead {
				fmt.Printf("worker: task moved to DLQ id=%s\n", t.ID)
			} else {
				metrics.Default.IncRescheduled()
				fmt.Printf("worker: task failed; rescheduled id=%s\n", t.ID)
			}
			continue
		}

		if err := store.CompleteTask(ctx, t.ID); err != nil {
			fmt.Println("worker: complete error:", err)
			continue
		}
		metrics.Default.IncCompleted()

		fmt.Printf("worker: completed task id=%s\n", t.ID)
	}
}

func leaseExtender(ctx context.Context, store *queue.Store, t *queue.Task, interval time.Duration, done <-chan struct{}, stopped chan<- struct{}) {
	defer close(stopped)

	if t.LeaseExpiresAt == nil {
		return
	}
	lastLease := *t.LeaseExpiresAt

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Double check stop signal
			select {
			case <-done:
				return
			default:
			}
			ok, err := store.ExtendLease(ctx, t.ID, lastLease, interval*2)
			if err != nil {
				fmt.Println("worker: lease extend error:", err)
				continue
			}
			if ok {
				// Advance our local lease marker
				lastLease = time.Now().UTC().Add(interval * 2)
				fmt.Println("worker: lease extended for", t.ID)
			} else {
				// Could not extend (lease changed/expired). Another worker may reclaim it soon.
				fmt.Println("worker: lease extension failed, stopping for", t.ID)
				return
			}
		}
	}
}

func startHeartbeat(ctx context.Context, store *queue.Store, workerID string, every time.Duration) func() {
	ctxHB, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctxHB.Done():
				return
			case <-ticker.C:
				_ = store.HeartbeatWorker(ctxHB, workerID)
			}
		}
	}()
	return cancel
}

func startReclaimer(ctx context.Context, store *queue.Store, staleAfter, every time.Duration) func() {
	ctxRec, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctxRec.Done():
				return
			case <-ticker.C:
				if n, err := store.ReclaimExpiredFromStaleWorkers(ctxRec, staleAfter, 200); err == nil && n > 0 {
					fmt.Printf("worker: reclaimed %d tasks from stale workers\n", n)
				}
			}
		}
	}()
	return cancel
}
