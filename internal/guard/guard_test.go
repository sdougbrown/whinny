package guard

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/store"
)

var t0 = time.Unix(1_000_000, 0)

// emptyBucket builds a 2/min bucket with no tokens left.
func emptyBucket(t *testing.T) *RateBucket {
	t.Helper()
	b := NewRateBucket(Config{PerMinute: 2})
	if ok, _ := b.Take(t0); !ok {
		t.Fatal("first Take failed")
	}
	if ok, _ := b.Take(t0); !ok {
		t.Fatal("second Take failed")
	}
	return b
}

func TestDenialKinds(t *testing.T) {
	chain := NewChain(Config{PerMinute: 2})
	tests := []struct {
		name  string
		in    GuardInput
		kind  string
		text  string // substring the text must mention
		retry time.Duration
	}{
		{
			name:  "breaker",
			in:    GuardInput{BreakerLatched: true, Bucket: NewRateBucket(Config{PerMinute: 2}), Now: t0},
			kind:  KindRefusedBreaker,
			text:  "do not retry",
			retry: 0,
		},
		{
			name:  "rate-limit",
			in:    GuardInput{Bucket: emptyBucket(t), Now: t0},
			kind:  KindRefusedRateLimit,
			text:  "30s",
			retry: 30 * time.Second,
		},
		{
			name:  "quiet",
			in:    GuardInput{Bucket: NewRateBucket(Config{PerMinute: 2}), QuietUntil: t0.Unix() + 60, Now: t0},
			kind:  KindRefusedQuiet,
			text:  "quiet",
			retry: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, denied := chain.Evaluate(tc.in)
			if !denied {
				t.Fatal("want denial, got none")
			}
			if d.Kind != tc.kind {
				t.Errorf("Kind = %q, want %q", d.Kind, tc.kind)
			}
			if !strings.Contains(d.Text, tc.text) {
				t.Errorf("Text = %q, want it to mention %q", d.Text, tc.text)
			}
			if d.RetryAfter != tc.retry {
				t.Errorf("RetryAfter = %s, want %s", d.RetryAfter, tc.retry)
			}
		})
	}
}

func TestAllowPath(t *testing.T) {
	chain := NewChain(Config{PerMinute: 2})
	d, denied := chain.Evaluate(GuardInput{Bucket: NewRateBucket(Config{PerMinute: 2}), Now: t0})
	if denied {
		t.Fatalf("allow path denied: %+v", d)
	}
}

func TestBucketRefill(t *testing.T) {
	b := NewRateBucket(Config{PerMinute: 2}) // refill: 1 token per 30s

	if ok, wait := b.Take(t0); !ok || wait != 0 {
		t.Fatalf("Take t0 = (%v, %v), want (true, 0)", ok, wait)
	}
	if ok, _ := b.Take(t0); !ok {
		t.Fatal("Take t0 (2nd) failed")
	}
	if ok, wait := b.Take(t0); ok || wait != 30*time.Second {
		t.Errorf("Take t0 (3rd) = (%v, %v), want (false, 30s)", ok, wait)
	}
	// Half a refill period in: 0.5 tokens, 15s to the next.
	if ok, wait := b.Take(t0.Add(15 * time.Second)); ok || wait != 15*time.Second {
		t.Errorf("Take t0+15s = (%v, %v), want (false, 15s)", ok, wait)
	}
	// A full refill period in: one token available again.
	if ok, _ := b.Take(t0.Add(30 * time.Second)); !ok {
		t.Error("Take t0+30s failed, want true")
	}

	// Refill is capped at capacity.
	b2 := NewRateBucket(Config{PerMinute: 2})
	if ok, _ := b2.Take(t0); !ok {
		t.Fatal("b2 Take t0 failed")
	}
	later := t0.Add(10 * time.Hour)
	for i, want := range []bool{true, true, false} {
		ok, _ := b2.Take(later)
		if ok != want {
			t.Fatalf("b2 Take %d at t0+10h = %v, want %v", i+1, ok, want)
		}
	}
}

func TestOrdering(t *testing.T) {
	chain := NewChain(Config{PerMinute: 2})
	all := GuardInput{
		BreakerLatched: true,
		Bucket:         emptyBucket(t),
		QuietUntil:     t0.Unix() + 60,
		Now:            t0,
	}

	d, denied := chain.Evaluate(all)
	if !denied || d.Kind != KindRefusedBreaker {
		t.Fatalf("all tripped: (%+v, %v), want refused-breaker", d, denied)
	}

	noBreaker := all
	noBreaker.BreakerLatched = false
	d, denied = chain.Evaluate(noBreaker)
	if !denied || d.Kind != KindRefusedRateLimit {
		t.Fatalf("breaker clear: (%+v, %v), want refused-rate-limit", d, denied)
	}

	noBreaker.Bucket = NewRateBucket(Config{PerMinute: 2})
	d, denied = chain.Evaluate(noBreaker)
	if !denied || d.Kind != KindRefusedQuiet {
		t.Fatalf("bucket fresh: (%+v, %v), want refused-quiet", d, denied)
	}
}

func TestQuietBoundary(t *testing.T) {
	chain := NewChain(Config{PerMinute: 2})
	fresh := func() GuardInput {
		return GuardInput{Bucket: NewRateBucket(Config{PerMinute: 2}), Now: t0}
	}

	// QuietUntil == now: the quiet period is over.
	in := fresh()
	in.QuietUntil = t0.Unix()
	if d, denied := chain.Evaluate(in); denied {
		t.Errorf("QuietUntil == now denied: %+v", d)
	}

	// QuietUntil one second ahead: still quiet.
	in = fresh()
	in.QuietUntil = t0.Unix() + 1
	d, denied := chain.Evaluate(in)
	if !denied || d.Kind != KindRefusedQuiet {
		t.Errorf("QuietUntil = now+1: (%+v, %v), want refused-quiet", d, denied)
	}

	// QuietUntil 0: never quiet.
	in = fresh()
	if d, denied := chain.Evaluate(in); denied {
		t.Errorf("QuietUntil 0 denied: %+v", d)
	}
}

// TestBreakerFromReopenedStore feeds the persisted breaker state from a
// store that was closed and reopened into the chain.
func TestBreakerFromReopenedStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	b := store.Breaker{Latched: true, LatchedAt: 42, Reason: "server rate limit"}
	if err := s.SetBreaker(context.Background(), b); err != nil {
		t.Fatalf("SetBreaker: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := store.Open(path)
	if err != nil {
		t.Fatalf("store reopen: %v", err)
	}
	defer s2.Close()
	persisted, err := s2.GetBreaker(context.Background())
	if err != nil {
		t.Fatalf("GetBreaker: %v", err)
	}
	if !persisted.Latched {
		t.Fatal("breaker not latched after reopen")
	}

	chain := NewChain(Config{PerMinute: 2})
	d, denied := chain.Evaluate(GuardInput{BreakerLatched: persisted.Latched, Now: t0})
	if !denied || d.Kind != KindRefusedBreaker {
		t.Fatalf("Evaluate: (%+v, %v), want refused-breaker", d, denied)
	}
}
