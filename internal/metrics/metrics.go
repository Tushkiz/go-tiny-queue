package metrics

import (
	"sync/atomic"
	"time"
)

type Counter struct {
	Leased      uint64
	Completed   uint64
	Failed      uint64
	Rescheduled uint64
}

type Metrics struct {
	leased      atomic.Uint64
	completed   atomic.Uint64
	failed      atomic.Uint64
	rescheduled atomic.Uint64
}

func (m *Metrics) IncLeased()      { m.leased.Add(1) }
func (m *Metrics) IncCompleted()   { m.completed.Add(1) }
func (m *Metrics) IncFailed()      { m.failed.Add(1) }
func (m *Metrics) IncRescheduled() { m.rescheduled.Add(1) }

func (m *Metrics) Snapshot() Counter {
	return Counter{
		Leased:      m.leased.Load(),
		Completed:   m.completed.Load(),
		Failed:      m.failed.Load(),
		Rescheduled: m.rescheduled.Load(),
	}
}

var Default Metrics

// Utility: returns a function to call on a ticker to print metrics somewhere else
func Every(d time.Duration, f func()) func() {
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				f()
			}
		}
	}()
	return func() { close(stop) }
}
