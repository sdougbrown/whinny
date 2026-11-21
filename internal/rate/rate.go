// Package rate holds the token-bucket math shared by the guard's
// in-memory bucket and the store's persisted take: one refill, admission,
// and wait computation, so the two admission paths cannot drift.
package rate

import "time"

// State is a token bucket's state. A zero Last means the bucket was
// never set and starts full.
type State struct {
	Tokens float64
	Last   time.Time
}

// Epsilon absorbs float drift in the tokens comparison: a bucket is
// "full enough" to admit at tokens >= Epsilon.
const Epsilon = 1 - 1e-9

// Refill advances s to now: tokens regrow at perMinute per minute, capped
// at capacity; Last advances only when now is strictly after it, so a
// clock step backward never refunds refill time. perMinute <= 0 is
// fail-closed (ok false: no tokens, no refill).
func Refill(s State, perMinute int, now time.Time) (State, bool) {
	if perMinute <= 0 {
		return State{}, false
	}
	if s.Last.IsZero() {
		return State{Tokens: float64(perMinute), Last: now}, true
	}
	if now.After(s.Last) {
		s.Tokens += now.Sub(s.Last).Seconds() / (time.Minute / time.Duration(perMinute)).Seconds()
		if s.Tokens > float64(perMinute) {
			s.Tokens = float64(perMinute)
		}
		s.Last = now
	}
	return s, true
}

// Available reports whether the refilled state can serve a token and,
// when it cannot, the retry-after wait (0 on admit).
func Available(s State, perMinute int) (bool, time.Duration) {
	if s.Tokens >= Epsilon {
		return true, 0
	}
	return false, time.Duration((1 - s.Tokens) * (time.Minute / time.Duration(perMinute)).Seconds() * float64(time.Second))
}

// Take consumes one token from the state.
func Take(s State) State {
	s.Tokens--
	return s
}

// Refund returns one token to the state, capped at the capacity, so a
// failed send (a take that never became a message) does not burn budget.
// Best-effort by contract: cross-process refunds can over- or
// under-count by one token under a race.
func Refund(s State, perMinute int) State {
	s.Tokens++
	if s.Tokens > float64(perMinute) {
		s.Tokens = float64(perMinute)
	}
	return s
}
