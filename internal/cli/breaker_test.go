package cli

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/guard"
	"github.com/sdougbrown/whinny/internal/store"
)

// breakerDeps builds postDeps with a high rate (so the bucket never
// denies) and a fixed clock, for the breaker trip tests.
func breakerDeps(t *testing.T, c *fakeClient) Deps {
	t.Helper()
	d := postDeps(t, c)
	d.Env = func(k string) string {
		if k == "WHINNY_RATE_PER_MINUTE" {
			return "10"
		}
		return ""
	}
	now := time.Unix(2_000_000, 0)
	d.Now = func() time.Time { return now }
	return d
}

// postOnce runs one post against the handler.
func postOnce(t *testing.T, d Deps) error {
	t.Helper()
	_, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	return err
}

// TestPostBreakerTripsOnRateLimitDenials: five consecutive
// refused-rate-limit post attempts (a pre-exhausted bucket) latch the
// breaker on the fifth — the trip itself refuses the post with the new
// latch — and the sixth post is refused by the persisted latch.
func TestPostBreakerTripsOnRateLimitDenials(t *testing.T) {
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1", Title: "General"}},
		postID: "msg-1",
	}
	d := postDeps(t, c)
	d.Env = func(k string) string {
		if k == "WHINNY_RATE_PER_MINUTE" {
			return "5"
		}
		return ""
	}
	now := time.Unix(2_000_000, 0)
	d.Now = func() time.Time { return now }
	if err := d.Store.SetRateState(context.Background(), 0, now); err != nil {
		t.Fatalf("SetRateState: %v", err)
	}
	for i := 1; i <= 4; i++ {
		err := postOnce(t, d)
		if err == nil || !strings.Contains(err.Error(), "refused-rate-limit") {
			t.Fatalf("post %d err = %v, want a refused-rate-limit denial", i, err)
		}
	}
	err := postOnce(t, d)
	if err == nil || !strings.Contains(err.Error(), "refused-breaker") {
		t.Fatalf("post 5 err = %v, want the trip to refuse with refused-breaker", err)
	}
	if !strings.Contains(err.Error(), "5 consecutive bad send outcomes") {
		t.Fatalf("post 5 err = %v, want the trip reason", err)
	}
	err = postOnce(t, d)
	if err == nil || !strings.Contains(err.Error(), "refused-breaker") {
		t.Fatalf("post 6 err = %v, want a refused-breaker denial", err)
	}
	if !strings.Contains(err.Error(), "5 consecutive bad send outcomes") {
		t.Fatalf("post 6 err = %v, want the persisted latch reason", err)
	}
	if len(c.posts) != 0 {
		t.Fatalf("posts = %d, want 0 (no send while denied or latched)", len(c.posts))
	}
}

// TestPostBreakerTripsOnPostFailures: five consecutive failed posts
// (a failing RoomClient) latch the breaker on the fifth with the
// outcome-neutral reason; the sixth post is refused by the persisted
// latch.
func TestPostBreakerTripsOnPostFailures(t *testing.T) {
	c := &fakeClient{
		joined:  []apronws.Room{{ID: "r1", Title: "General"}},
		postID:  "msg-1",
		postErr: errors.New("send failed"),
	}
	d := breakerDeps(t, c)
	for i := 1; i <= 4; i++ {
		err := postOnce(t, d)
		if err == nil || !strings.Contains(err.Error(), "send failed") {
			t.Fatalf("post %d err = %v, want the transport failure", i, err)
		}
	}
	err := postOnce(t, d)
	if err == nil || !strings.Contains(err.Error(), "refused-breaker") {
		t.Fatalf("post 5 err = %v, want the trip to refuse with refused-breaker", err)
	}
	if !strings.Contains(err.Error(), "5 consecutive bad send outcomes") {
		t.Fatalf("post 5 err = %v, want the trip reason", err)
	}
	err = postOnce(t, d)
	if err == nil || !strings.Contains(err.Error(), "refused-breaker") {
		t.Fatalf("post 6 err = %v, want a refused-breaker denial", err)
	}
	if len(c.posts) != 0 {
		t.Fatalf("posts = %d, want 0 (every post failed or was refused)", len(c.posts))
	}
}

// TestPostSuccessResetsFailureCounter: a confirmed send resets the
// consecutive failure counter — four failures, a success, four more
// failures — so the breaker does not latch.
func TestPostSuccessResetsFailureCounter(t *testing.T) {
	c := &fakeClient{
		joined:  []apronws.Room{{ID: "r1", Title: "General"}},
		postID:  "msg-1",
		postErr: errors.New("send failed"),
	}
	d := breakerDeps(t, c)
	fail := func() {
		t.Helper()
		err := postOnce(t, d)
		if err == nil || !strings.Contains(err.Error(), "send failed") {
			t.Fatalf("err = %v, want the transport failure", err)
		}
	}
	for i := 0; i < 4; i++ {
		fail()
	}
	c.postErr = nil
	if err := postOnce(t, d); err != nil {
		t.Fatalf("success post: %v", err)
	}
	c.postErr = errors.New("send failed")
	for i := 0; i < 4; i++ {
		fail()
	}
	brk, err := d.Store.GetBreaker(context.Background())
	if err != nil {
		t.Fatalf("GetBreaker: %v", err)
	}
	if brk.Latched {
		t.Fatal("breaker latched after a success reset the counter")
	}
	// The tenth post (the bucket's last token) is allowed and
	// confirmed: the counter is 4, below the threshold.
	c.postErr = nil
	if err := postOnce(t, d); err != nil {
		t.Fatalf("tenth post: %v", err)
	}
	if len(c.posts) != 2 {
		t.Fatalf("posts = %d, want 2 (the two confirmed sends)", len(c.posts))
	}
}

// TestPostQuietDenialDoesNotBump: refused-quiet denials are operator
// silence, not bad send outcomes — they do not bump the failure
// counter or latch the breaker, even at the trip threshold.
func TestPostQuietDenialDoesNotBump(t *testing.T) {
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1", Title: "General"}},
		postID: "msg-1",
	}
	d := postDeps(t, c)
	now := time.Unix(2_000_000, 0)
	d.Now = func() time.Time { return now }
	if err := d.Store.SetMeta(context.Background(), MetaQuietUntil, "9999999999"); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	for i := 0; i < guard.BreakerTripThreshold; i++ {
		err := postOnce(t, d)
		if err == nil || !strings.Contains(err.Error(), "refused-quiet") {
			t.Fatalf("post %d err = %v, want a refused-quiet denial", i+1, err)
		}
	}
	v, err := d.Store.GetMeta(context.Background(), store.MetaPostFailures)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if v != "" && v != "0" {
		t.Fatalf("post_failures = %q, want unset (a quiet denial does not bump)", v)
	}
	brk, err := d.Store.GetBreaker(context.Background())
	if err != nil {
		t.Fatalf("GetBreaker: %v", err)
	}
	if brk.Latched {
		t.Fatal("breaker latched after quiet denials")
	}
}

// TestBreakerLatchPersistsAcrossReopen: a latched breaker persists in
// the store — a post on a reopened store (a new handler invocation) is
// still refused with the persisted reason.
func TestBreakerLatchPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	c := &fakeClient{
		joined:  []apronws.Room{{ID: "r1", Title: "General"}},
		postID:  "msg-1",
		postErr: errors.New("send failed"),
	}
	d := Deps{
		Client: c,
		Store:  s,
		Env: func(k string) string {
			if k == "WHINNY_RATE_PER_MINUTE" {
				return "10"
			}
			return ""
		},
		Now: func() time.Time { return time.Unix(2_000_000, 0) },
	}
	for i := 0; i < guard.BreakerTripThreshold; i++ {
		if err := postOnce(t, d); err == nil {
			t.Fatalf("post %d: want a failure, got nil", i+1)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open (reopen): %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	d.Store = s2
	err = postOnce(t, d)
	if err == nil || !strings.Contains(err.Error(), "refused-breaker") {
		t.Fatalf("post after reopen err = %v, want a refused-breaker denial", err)
	}
	if !strings.Contains(err.Error(), "5 consecutive bad send outcomes") {
		t.Fatalf("post after reopen err = %v, want the persisted reason", err)
	}
}

// TestBreakerClear: breaker-clear releases the latch and resets the
// counter, so the next post is allowed.
func TestBreakerClear(t *testing.T) {
	c := &fakeClient{
		joined:  []apronws.Room{{ID: "r1", Title: "General"}},
		postID:  "msg-1",
		postErr: errors.New("send failed"),
	}
	d := breakerDeps(t, c)
	for i := 0; i < guard.BreakerTripThreshold; i++ {
		if err := postOnce(t, d); err == nil {
			t.Fatalf("post %d: want a failure, got nil", i+1)
		}
	}
	res, err := BreakerClearHandler(d)(context.Background(),
		inv("breaker-clear", nil, nil, nil), io.Discard)
	if err != nil {
		t.Fatalf("BreakerClearHandler: %v", err)
	}
	if res == nil || !strings.Contains(res.(string), "breaker cleared") {
		t.Fatalf("result = %v, want the cleared confirmation", res)
	}
	c.postErr = nil
	if err := postOnce(t, d); err != nil {
		t.Fatalf("post after clear: %v", err)
	}
	if len(c.posts) != 1 {
		t.Fatalf("posts = %d, want 1 (the post after the clear)", len(c.posts))
	}
}

// TestPostBreakerRefusalMatchesGuardFormat: the post-side trip refusal
// is formatted by the same helper as the guard chain's breaker denial:
// for the same reason the post's error text is exactly the chain's
// denial text with the kind prefix.
func TestPostBreakerRefusalMatchesGuardFormat(t *testing.T) {
	c := &fakeClient{
		joined:  []apronws.Room{{ID: "r1", Title: "General"}},
		postID:  "msg-1",
		postErr: errors.New("send failed"),
	}
	d := breakerDeps(t, c)
	for i := 0; i < guard.BreakerTripThreshold; i++ {
		if err := postOnce(t, d); err == nil {
			t.Fatalf("post %d: want a failure, got nil", i+1)
		}
	}
	brk, err := d.Store.GetBreaker(context.Background())
	if err != nil {
		t.Fatalf("GetBreaker: %v", err)
	}
	if !brk.Latched {
		t.Fatal("breaker not latched after the trip")
	}
	// The chain's denial for the same persisted reason.
	chain := guard.NewChain(guard.Config{PerMinute: 10})
	denial, denied := chain.Evaluate(guard.GuardInput{
		BreakerLatched: brk.Latched,
		BreakerReason:  brk.Reason,
		Now:            time.Unix(2_000_000, 0),
	})
	if !denied || denial.Kind != guard.KindRefusedBreaker {
		t.Fatalf("chain Evaluate: (%+v, %v), want a breaker denial", denial, denied)
	}
	// The post-side refusal for the same reason.
	err = postOnce(t, d)
	if err == nil {
		t.Fatal("want a refusal, got nil")
	}
	want := guard.KindRefusedBreaker + ": " + denial.Text
	if err.Error() != want {
		t.Errorf("post refusal = %q, want %q (the guard's format for the same reason)", err.Error(), want)
	}
}

// TestPostRoomResolutionErrorDoesNotBump: a misspelled room is a usage
// error the caller can fix — it refunds without advancing the failure
// counter, so it can never contribute to a breaker trip.
func TestPostRoomResolutionErrorDoesNotBump(t *testing.T) {
	c := &fakeClient{
		joined:  []apronws.Room{{ID: "r1", Title: "General"}},
		postErr: errors.New("send failed"),
	}
	d := postDeps(t, c)
	now := time.Unix(2_000_000, 0)
	d.Now = func() time.Time { return now }

	badRoom := func() error {
		_, err := PostHandler(d)(context.Background(),
			inv("post", []string{"hello"}, map[string]string{"room": "nope"}, nil), io.Discard)
		return err
	}
	for i := 1; i <= 6; i++ {
		err := badRoom()
		if err == nil || !strings.Contains(err.Error(), "nope") {
			t.Fatalf("bad-room post %d err = %v, want the resolution error", i, err)
		}
	}

	// The counter is still at zero: five real post failures are needed
	// to trip, proving the resolution errors never counted.
	for i := 1; i <= 4; i++ {
		err := postOnce(t, d)
		if err == nil || !strings.Contains(err.Error(), "send failed") {
			t.Fatalf("failing post %d err = %v, want the transport error", i, err)
		}
	}
	err := postOnce(t, d)
	if err == nil || !strings.Contains(err.Error(), "refused-breaker") {
		t.Fatalf("failing post 5 err = %v, want the trip to refuse with refused-breaker", err)
	}
	if len(c.posts) != 0 {
		t.Fatalf("posts = %d, want 0", len(c.posts))
	}
}
