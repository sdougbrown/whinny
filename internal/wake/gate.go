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
// per-hour cap.
func NewAmbientGate(minSpacing time.Duration, maxPerHour int) *AmbientGate {
	return &AmbientGate{MinSpacing: minSpacing, MaxPerHour: maxPerHour}
}

// Allow reports whether an ambient wake is permitted at now. It is
// blocked if the last ambient wake was less than MinSpacing ago, or if
// there are already MaxPerHour ambient wakes in the trailing hour.
func (g *AmbientGate) Allow(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prune(now)
	if g.hasLast && now.Sub(g.last) < g.MinSpacing {
		return false
	}
	if g.MaxPerHour >= 0 && len(g.wakes) >= g.MaxPerHour {
		return false
	}
	return true
}

// Record marks an ambient wake at now.
func (g *AmbientGate) Record(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.last = now
	g.hasLast = true
	g.prune(now)
	g.wakes = append(g.wakes, now)
}

// prune drops wake timestamps that are not within the trailing hour.
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
