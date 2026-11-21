package wake

import (
	"sync"
	"time"
)

// AmbientGate bounds ambient wakes with a minimum spacing between ambient
// wakes and a per-hour cap. Its state (the last ambient wake and the
// trailing-hour wake timestamps) is in-memory and owned by the daemon.
type AmbientGate struct {
	mu         sync.Mutex
	MinSpacing time.Duration
	MaxPerHour int
	last       time.Time
	hasLast    bool
	wakes      []time.Time
}

// NewAmbientGate returns an AmbientGate with the given min spacing and
// per-hour cap. A negative cap is treated as 0: every ambient wake is
// denied.
func NewAmbientGate(minSpacing time.Duration, maxPerHour int) *AmbientGate {
	if maxPerHour < 0 {
		maxPerHour = 0
	}
	return &AmbientGate{MinSpacing: minSpacing, MaxPerHour: maxPerHour}
}

// TryReserve checks the valves (min spacing, per-hour cap) and, when
// they pass, records the wake — atomically under the mutex. It returns
// false without recording when a valve blocks.
func (g *AmbientGate) TryReserve(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prune(now)
	if g.hasLast && now.Sub(g.last) < g.MinSpacing {
		return false
	}
	if len(g.wakes) >= g.MaxPerHour {
		return false
	}
	g.last = now
	g.hasLast = true
	g.wakes = append(g.wakes, now)
	return true
}

// Release removes the reservation made at now (a classifier definitive
// no), restoring the spacing and trailing-hour state.
func (g *AmbientGate) Release(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	removed := -1
	for i, t := range g.wakes {
		if t.Equal(now) {
			removed = i
			break
		}
	}
	if removed < 0 {
		return
	}
	g.wakes = append(g.wakes[:removed], g.wakes[removed+1:]...)
	if g.last.Equal(now) {
		var newest time.Time
		for _, t := range g.wakes {
			if t.After(newest) {
				newest = t
			}
		}
		if newest.IsZero() {
			g.last = time.Time{}
			g.hasLast = false
		} else {
			g.last = newest
		}
	}
}

// prune drops wake timestamps that are not within the trailing hour.
// The caller must hold g.mu.
func (g *AmbientGate) prune(now time.Time) {
	cutoff := now.Add(-time.Hour)
	n := 0
	for _, t := range g.wakes {
		if t.After(cutoff) {
			g.wakes[n] = t
			n++
		}
	}
	g.wakes = g.wakes[:n]
}
