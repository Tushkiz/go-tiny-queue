package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tushkiz/go-tiny-queue/internal/events"
	"github.com/tushkiz/go-tiny-queue/internal/queue"
	"github.com/tushkiz/go-tiny-queue/internal/util"
)

func main() {
	ctx := context.Background()

	dsn := util.Getenv("DB_DSN", "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC")
	store, err := queue.NewStore(dsn)
	if err != nil {
		log.Fatalf("adminapi: store init failed: %v", err)
	}

	mux := http.NewServeMux()
	hub := events.NewHub(64)
	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		status := r.URL.Query().Get("status")
		queueName := r.URL.Query().Get("queue")
		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				limit = n
			}
		}
		tasks, err := store.ListTasksFiltered(ctx, status, queueName, limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
	})

	mux.HandleFunc("/api/tasks/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
		parts := strings.Split(path, "/")
		if len(parts) == 0 || parts[0] == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing id"})
			return
		}
		id := parts[0]

		// Events endpoints
		if len(parts) >= 2 && parts[1] == "events" {
			// Publish (producer emits event)
			if r.Method == http.MethodPost && len(parts) == 2 {
				var body struct {
					Level   string          `json:"level"`
					Kind    string          `json:"kind"`
					Message string          `json:"message"`
					Data    json.RawMessage `json:"data"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
					return
				}
				ev := events.Event{
					TaskID:  id,
					TS:      time.Now().UTC(),
					Level:   body.Level,
					Kind:    body.Kind,
					Message: body.Message,
					Data:    body.Data,
				}
				hub.Publish(ev)
				writeJSON(w, http.StatusOK, map[string]any{"ok": true})
				return
			}
			// SSE stream
			if r.Method == http.MethodGet && len(parts) == 3 && parts[2] == "stream" {
				flusher, ok := w.(http.Flusher)
				if !ok {
					http.Error(w, "streaming unsupported", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				w.Header().Set("Connection", "keep-alive")
				ch, unsub := hub.Subscribe(id)
				defer unsub()
				enc := json.NewEncoder(w)
				// Send a hello comment
				fmt.Fprintf(w, ": subscribed to %s\n\n", id)
				flusher.Flush()
				for {
					select {
					case <-r.Context().Done():
						return
					case ev := <-ch:
						// SSE event with kind as event type, JSON payload in data
						if ev.Kind != "" {
							fmt.Fprintf(w, "event: %s\n", ev.Kind)
						}
						fmt.Fprintf(w, "data: ")
						_ = enc.Encode(ev)
						fmt.Fprintf(w, "\n")
						flusher.Flush()
					}
				}
			}
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
			return
		}
		if len(parts) == 1 && r.Method == http.MethodGet {
			t, err := store.GetTask(ctx, id)
			if err != nil {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
				return
			}
			writeJSON(w, http.StatusOK, t)
			return
		}
		if len(parts) == 2 && r.Method == http.MethodPost {
			switch parts[1] {
			case "pause":
				var body struct {
					PauseFor string `json:"pause_for"`
					Until   string `json:"until"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
					return
				}
				var until time.Time
				if body.PauseFor != "" {
					dur, err := time.ParseDuration(body.PauseFor)
					if err != nil || dur <= 0 {
						writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid pause_for"})
						return
					}
					until = time.Now().UTC().Add(dur)
				} else if body.Until != "" {
					tm, err := time.Parse(time.RFC3339Nano, body.Until)
					if err != nil {
						writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid until"})
						return
					}
					until = tm.UTC()
				} else {
					writeJSON(w, http.StatusBadRequest, map[string]any{"error": "pause_for or until required"})
					return
				}
				if err := store.PauseTask(ctx, id, until); err != nil {
					writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
					return
				}
				writeJSON(w, http.StatusOK, map[string]any{"ok": true, "paused_until": until.Format(time.RFC3339Nano)})
			case "resume":
				if err := store.ResumeTask(ctx, id); err != nil {
					writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
					return
				}
				writeJSON(w, http.StatusOK, map[string]any{"ok": true})
			case "cancel":
				if err := store.CancelTask(ctx, id); err != nil {
					writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
					return
				}
				writeJSON(w, http.StatusOK, map[string]any{"ok": true})
			default:
				writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown action"})
			}
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
	})

	addr := util.Getenv("ADMIN_LISTEN", ":8888")
	srv := &http.Server{
		Addr:              addr,
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Println("adminapi: listening on", addr)
	log.Fatal(srv.ListenAndServe())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		fmt.Printf("%s %s -> %s (%s)\n", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
	})
}
