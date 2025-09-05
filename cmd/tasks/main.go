package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tushkiz/go-tiny-queue/internal/queue"
	"github.com/tushkiz/go-tiny-queue/internal/util"
)

func main() {
	ctx := context.Background()

	var (
		status = flag.String("status", "all", "Task status to filter: pending | in_flight | completed | failed | dead | all")
		limit  = flag.Int("limit", 50, "Maximum number of tasks to list (max 500)")
		asJSON = flag.Bool("json", false, "Output as JSON")
	)
	flag.Parse()

	dsn := util.Getenv("DB_DSN", "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC")
	store, err := queue.NewStore(dsn)
	if err != nil {
		panic(err)
	}

	tasks, err := store.ListTasks(ctx, *status, *limit)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: ", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", " ")
		_ = enc.Encode(tasks)
		return
	}

	// Pretty print table
	fmt.Printf("Listing %d task(s) (status=%s)\n\n", len(tasks), *status)
	fmt.Printf("%-36s  %-10s  %-8s  %-11s  %-26s  %-26s\n", "ID", "TYPE", "ATTEMPT", "STATUS", "NEXT_RUN_AT", "UPDATED_AT")
	fmt.Println("-----------------------------------------------------------------------------------------------")
	for _, t := range tasks {
		fmt.Printf(
			"%-36s  %-10s  %8d  %-11s  %-26s  %-26s\n",
			t.ID,
			t.Type,
			t.Attempt,
			t.Status,
			t.NextRunAt.UTC().Format(time.RFC3339Nano),
			t.UpdatedAt.UTC().Format(time.RFC3339Nano),
		)
	}

}
