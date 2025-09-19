package events

import (
	"encoding/json"
	"sync"
	"time"
)

// Event is a neutral envelope for task-level events.
// Data can be any JSON payload provided by the producer.
type Event struct {
	TaskID  string          `json:"task_id"`
	TS      time.Time       `json:"ts"`
	Level   string          `json:"level"`  // info | warn | error | debug
	Kind    string          `json:"kind"`   // freeform classifier (e.g., llm_token, tool_call)
	Message string          `json:"message,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Hub is an in-memory pub/sub broker for task events. Not process-safe.
// Intended for the adminapi process only in this milestone.
type Hub struct {
	mu       sync.RWMutex
	subs     map[string]map[chan Event]struct{} // taskID -> set of subscriber channels
	bufSize  int
}

func NewHub(bufSize int) *Hub {
	return &Hub{
		subs:    make(map[string]map[chan Event]struct{}),
		bufSize: bufSize,
	}
}

// Subscribe returns a channel and an unsubscribe function.
func (h *Hub) Subscribe(taskID string) (<-chan Event, func()) {
	ch := make(chan Event, h.bufSize)
	h.mu.Lock()
	defer h.mu.Unlock()
	set := h.subs[taskID]
	if set == nil {
		set = make(map[chan Event]struct{})
		h.subs[taskID] = set
	}
	set[ch] = struct{}{}
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if set, ok := h.subs[taskID]; ok {
			delete(set, ch)
			if len(set) == 0 {
				delete(h.subs, taskID)
			}
		}
		close(ch)
	}
}

// Publish sends an Event to all subscribers of its TaskID.
func (h *Hub) Publish(ev Event) {
	h.mu.RLock()
	set := h.subs[ev.TaskID]
	// copy keys to avoid holding lock while sending
	chs := make([]chan Event, 0, len(set))
	for ch := range set {
		chs = append(chs, ch)
	}
	h.mu.RUnlock()
	for _, ch := range chs {
		select {
		case ch <- ev:
		default:
			// drop if subscriber is slow
		}
	}
}
