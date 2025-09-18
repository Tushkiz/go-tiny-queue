package queue

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

// setupTestStore creates a Store connected to a MySQL instance and ensures the schema exists.
// If the DB is unavailable, tests will be skipped with an explanatory message.
func setupTestStore(t *testing.T) *Store {
	t.Helper()
	// Prefer TEST_DB_DSN for tests, fall back to DB_DSN, then project default
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = os.Getenv("DB_DSN")
	}
	if dsn == "" {
		dsn = "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC"
	}

	store, err := NewStore(dsn)
	if err != nil {
		t.Skipf("skipping: cannot connect to DB: %v (run `docker compose up -d db` and `make goose`)", err)
	}

	// Ping to ensure DB is alive
	sqlDB, err := store.DB.DB()
	if err != nil {
		t.Skipf("skipping: cannot get sql DB: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Skipf("skipping: DB not reachable: %v (run `docker compose up -d db` and `make goose`)", err)
	}

	// Ensure schema exists (only what tests need)
	if err := store.DB.AutoMigrate(&Task{}, &Worker{}); err != nil {
		t.Fatalf("automigrate failed: %v", err)
	}

	// Clean tables
	_ = store.DB.Exec("DELETE FROM tasks").Error
	_ = store.DB.Exec("DELETE FROM workers").Error

	return store
}

func TestPauseResumeAffectsLeasing(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Register worker
	if err := store.RegisterWorker(ctx, "w1"); err != nil {
		t.Fatalf("register worker w1 failed: %v", err)
	}

	// Enqueue in dedicated control queue
	tCtl, err := store.Enqueue(ctx, "type.ctrl", map[string]any{"n": 1}, WithQueueName("qctl"))
	if err != nil {
		t.Fatalf("enqueue ctl failed: %v", err)
	}

	// Pause for 10 minutes
	until := time.Now().UTC().Add(10 * time.Minute)
	if err := store.PauseTask(ctx, tCtl.ID, until); err != nil {
		t.Fatalf("pause task failed: %v", err)
	}

	// Should not lease while paused
	_, err = store.FetchAndLeaseFromQueues(ctx, "w1", 10*time.Second, []string{"qctl"})
	if err == nil {
		t.Fatalf("expected no rows while paused, got task")
	}
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows while paused, got %T %v", err, err)
	}

	// Resume and lease should succeed
	if err := store.ResumeTask(ctx, tCtl.ID); err != nil {
		t.Fatalf("resume task failed: %v", err)
	}
	leased, err := store.FetchAndLeaseFromQueues(ctx, "w1", 10*time.Second, []string{"qctl"})
	if err != nil {
		t.Fatalf("lease after resume failed: %v", err)
	}
	if leased == nil || leased.ID != tCtl.ID {
		t.Fatalf("leased wrong task after resume: got %+v want ID=%s", leased, tCtl.ID)
	}
}

func TestCancelAffectsLeasing(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Register worker
	if err := store.RegisterWorker(ctx, "w1"); err != nil {
		t.Fatalf("register worker w1 failed: %v", err)
	}

	// Enqueue and immediately cancel
	tCtl, err := store.Enqueue(ctx, "type.ctrl2", map[string]any{"n": 2}, WithQueueName("qctl2"))
	if err != nil {
		t.Fatalf("enqueue ctl2 failed: %v", err)
	}
	if err := store.CancelTask(ctx, tCtl.ID); err != nil {
		t.Fatalf("cancel task failed: %v", err)
	}

	// Should not lease canceled tasks
	_, err = store.FetchAndLeaseFromQueues(ctx, "w1", 10*time.Second, []string{"qctl2"})
	if err == nil {
		t.Fatalf("expected no rows for canceled task, got task")
	}
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows for canceled task, got %T %v", err, err)
	}
}

func TestEnqueueWithQueueName(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Default queue
	t1, err := store.Enqueue(ctx, "type.default", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("enqueue default queue failed: %v", err)
	}
	if got, want := t1.QueueName, "default"; got != want {
		t.Fatalf("QueueName mismatch: got %q want %q", got, want)
	}

	// Custom queue
	qname := "qA"
	t2, err := store.Enqueue(ctx, "type.custom", map[string]any{"y": true}, WithQueueName(qname))
	if err != nil {
		t.Fatalf("enqueue custom queue failed: %v", err)
	}
	if got, want := t2.QueueName, qname; got != want {
		t.Fatalf("QueueName mismatch: got %q want %q", got, want)
	}
}

func TestFetchAndLeaseFromQueues_Filters(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Register workers
	if err := store.RegisterWorker(ctx, "w1"); err != nil {
		t.Fatalf("register worker w1 failed: %v", err)
	}
	if err := store.RegisterWorker(ctx, "w2"); err != nil {
		t.Fatalf("register worker w2 failed: %v", err)
	}

	// Enqueue across two queues
	tQ1, err := store.Enqueue(ctx, "type.t1", map[string]any{"n": 1}, WithQueueName("q1"))
	if err != nil {
		t.Fatalf("enqueue q1 failed: %v", err)
	}
	_, err = store.Enqueue(ctx, "type.t2", map[string]any{"n": 2}, WithQueueName("q2"))
	if err != nil {
		t.Fatalf("enqueue q2 failed: %v", err)
	}

	// Lease only from q1
	t1, err := store.FetchAndLeaseFromQueues(ctx, "w1", 10*time.Second, []string{"q1"})
	if err != nil {
		t.Fatalf("FetchAndLeaseFromQueues q1 failed: %v", err)
	}
	if t1 == nil || t1.ID != tQ1.ID {
		t.Fatalf("leased wrong task: got %+v want ID=%s", t1, tQ1.ID)
	}
	if t1.QueueName != "q1" {
		t.Fatalf("leased from wrong queue: got %q want %q", t1.QueueName, "q1")
	}

	// Next lease from q1 should be empty (no more tasks in q1)
	_, err = store.FetchAndLeaseFromQueues(ctx, "w1", 10*time.Second, []string{"q1"})
	if err == nil {
		t.Fatalf("expected no rows for q1, got task")
	}
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got %T %v", err, err)
	}

	// Lease from q2 should succeed
	t2, err := store.FetchAndLeaseFromQueues(ctx, "w2", 10*time.Second, []string{"q2"})
	if err != nil {
		t.Fatalf("FetchAndLeaseFromQueues q2 failed: %v", err)
	}
	if t2.QueueName != "q2" {
		t.Fatalf("leased from wrong queue: got %q want %q", t2.QueueName, "q2")
	}
}
