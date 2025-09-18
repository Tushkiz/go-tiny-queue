package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tushkiz/go-tiny-queue/internal/queue"
	"github.com/tushkiz/go-tiny-queue/internal/util"
)

func main() {
	ctx := context.Background()

	id := flag.String("id", "", "task id (required)")
	pauseFor := flag.String("pause-for", "", "duration to pause (e.g. 15m, 1h). If set, pauses the task")
	resume := flag.Bool("resume", false, "resume a paused task")
	cancel := flag.Bool("cancel", false, "cancel a task")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s -id <task_id> [ -pause-for <duration> | -resume | -cancel ]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if *id == "" {
		flag.Usage()
		os.Exit(2)
	}

	actions := 0
	if *pauseFor != "" {
		actions++
	}
	if *resume {
		actions++
	}
	if *cancel {
		actions++
	}
	if actions != 1 {
		fmt.Fprintln(os.Stderr, "error: exactly one of -pause-for, -resume, -cancel must be specified")
		flag.Usage()
		os.Exit(2)
	}

	dsn := util.Getenv("DB_DSN", "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC")
	store, err := queue.NewStore(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: ", err)
		os.Exit(1)
	}

	switch {
	case *pauseFor != "":
		dur, err := time.ParseDuration(*pauseFor)
		if err != nil || dur <= 0 {
			fmt.Fprintf(os.Stderr, "invalid -pause-for duration: %q\n", *pauseFor)
			os.Exit(2)
		}
		until := time.Now().UTC().Add(dur)
		if err := store.PauseTask(ctx, *id, until); err != nil {
			fmt.Fprintln(os.Stderr, "pause error:", err)
			os.Exit(1)
		}
		fmt.Printf("paused task %s until %s\n", *id, until.Format(time.RFC3339Nano))
	case *resume:
		if err := store.ResumeTask(ctx, *id); err != nil {
			fmt.Fprintln(os.Stderr, "resume error:", err)
			os.Exit(1)
		}
		fmt.Printf("resumed task %s\n", *id)
	case *cancel:
		if err := store.CancelTask(ctx, *id); err != nil {
			fmt.Fprintln(os.Stderr, "cancel error:", err)
			os.Exit(1)
		}
		fmt.Printf("canceled task %s\n", *id)
	}
}
