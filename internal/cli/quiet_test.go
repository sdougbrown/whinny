package cli

import (
	"context"
	"io"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/store"
)

// quietDeps builds Deps with a temp-dir store and an injected clock.
func quietDeps(t *testing.T) (Deps, *store.Store, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Unix(2_000_000, 0)
	d := Deps{
		Store: s,
		Env:   func(string) string { return "" },
		Now:   func() time.Time { return now },
	}
	return d, s, now
}

// TestQuietSetsGate: `quiet --for 1h` sets quiet_until to now + 1h.
func TestQuietSetsGate(t *testing.T) {
	d, s, now := quietDeps(t)
	i := inv("quiet", nil, nil, nil)
	i.Durations = map[string]time.Duration{"for": time.Hour}
	i.Supplied = map[string]bool{"for": true}
	res, err := QuietHandler(d)(context.Background(), i, io.Discard)
	if err != nil {
		t.Fatalf("QuietHandler: %v", err)
	}
	v, err := s.GetMeta(context.Background(), MetaQuietUntil)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	want := now.Add(time.Hour).Unix()
	if v != strconv.FormatInt(want, 10) {
		t.Fatalf("quiet_until = %s, want %d", v, want)
	}
	if res == "" {
		t.Fatal("want a confirmation message")
	}
}

// TestQuietDefaultDuration: `quiet` with no --for uses the 8h default.
func TestQuietDefaultDuration(t *testing.T) {
	d, s, now := quietDeps(t)
	res, err := QuietHandler(d)(context.Background(), inv("quiet", nil, nil, nil), io.Discard)
	if err != nil {
		t.Fatalf("QuietHandler: %v", err)
	}
	v, _ := s.GetMeta(context.Background(), MetaQuietUntil)
	want := now.Add(DefaultQuietDuration).Unix()
	if v != strconv.FormatInt(want, 10) {
		t.Fatalf("quiet_until = %s, want %d (the 8h default)", v, want)
	}
	if res == "" {
		t.Fatal("want a confirmation message")
	}
}

// TestQuietIndefinite: `quiet --for 0` stores the indefinite sentinel.
func TestQuietIndefinite(t *testing.T) {
	d, s, _ := quietDeps(t)
	i := inv("quiet", nil, nil, nil)
	i.Durations = map[string]time.Duration{"for": 0}
	i.Supplied = map[string]bool{"for": true}
	res, err := QuietHandler(d)(context.Background(), i, io.Discard)
	if err != nil {
		t.Fatalf("QuietHandler: %v", err)
	}
	v, _ := s.GetMeta(context.Background(), MetaQuietUntil)
	if v != strconv.FormatInt(QuietUntilIndefinite, 10) {
		t.Fatalf("quiet_until = %s, want the indefinite sentinel %d", v, QuietUntilIndefinite)
	}
	if res == "" {
		t.Fatal("want a confirmation message")
	}
}

// TestResumeClearsGate: `resume` clears the quiet gate.
func TestResumeClearsGate(t *testing.T) {
	d, s, _ := quietDeps(t)
	ctx := context.Background()
	// Arm the gate, then clear it.
	if err := s.SetMeta(ctx, MetaQuietUntil, strconv.FormatInt(QuietUntilIndefinite, 10)); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	res, err := ResumeHandler(d)(ctx, inv("resume", nil, nil, nil), io.Discard)
	if err != nil {
		t.Fatalf("ResumeHandler: %v", err)
	}
	v, err := s.GetMeta(ctx, MetaQuietUntil)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if v != "" {
		t.Fatalf("quiet_until = %q, want cleared (empty)", v)
	}
	if res == "" {
		t.Fatal("want a confirmation message")
	}
}
