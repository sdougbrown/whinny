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
	"github.com/sdougbrown/whinny/internal/store"
)

// postDeps builds Deps around a fake client with a temp-dir store (the
// send-path guard chain reads the store before connecting).
func postDeps(t *testing.T, c *fakeClient) Deps {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	d := testDeps(c)
	d.Store = s
	return d
}

func TestPostResolvesRoomAndPosts(t *testing.T) {
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1", Title: "General"}},
		postID: "msg-1",
	}
	res, err := PostHandler(postDeps(t, c))(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "general", "reply-to": "m1"}, nil), io.Discard)
	if err != nil {
		t.Fatalf("PostHandler: %v", err)
	}
	if res != "msg-1" {
		t.Fatalf("result = %v, want the message_id", res)
	}
	if len(c.posts) != 1 {
		t.Fatalf("posts = %d, want 1", len(c.posts))
	}
	p := c.posts[0]
	if p.RoomID != "r1" {
		t.Fatalf("RoomID = %q, want the resolved room id", p.RoomID)
	}
	if p.Body.Text != "hello" {
		t.Fatalf("Body.Text = %q, want hello", p.Body.Text)
	}
	if p.ReplyTo == nil || p.ReplyTo.MessageID != "m1" {
		t.Fatalf("ReplyTo = %+v, want m1", p.ReplyTo)
	}
	if len(c.joinedRooms) != 1 || c.joinedRooms[0] != "r1" {
		t.Fatalf("joinedRooms = %v, want [r1]", c.joinedRooms)
	}
}

func TestPostThreadOf(t *testing.T) {
	c := &fakeClient{
		joined:   []apronws.Room{{ID: "r1", Title: "General"}},
		postID:   "msg-1",
		threadID: "t1",
	}
	res, err := PostHandler(postDeps(t, c))(context.Background(),
		inv("post", []string{"First line\nsecond line"}, map[string]string{"room": "r1", "thread-of": "m2"}, map[string]bool{"json": true}), io.Discard)
	if err != nil {
		t.Fatalf("PostHandler: %v", err)
	}
	if len(c.threads) != 1 {
		t.Fatalf("threads = %d, want 1", len(c.threads))
	}
	th := c.threads[0]
	if th.ParentRoomID != "r1" {
		t.Fatalf("ParentRoomID = %q, want the parent room", th.ParentRoomID)
	}
	if th.Title != "First line" {
		t.Fatalf("Title = %q, want the first line", th.Title)
	}
	if len(c.posts) != 1 {
		t.Fatalf("posts = %d, want 1", len(c.posts))
	}
	p := c.posts[0]
	if p.RoomID != "t1" {
		t.Fatalf("RoomID = %q, want the thread room", p.RoomID)
	}
	if p.ReplyTo == nil || p.ReplyTo.MessageID != "m2" {
		t.Fatalf("ReplyTo = %+v, want the thread-of message", p.ReplyTo)
	}
	m, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("result = %T, want a JSON map", res)
	}
	if m["message_id"] != "msg-1" || m["room_id"] != "t1" {
		t.Fatalf("result = %v, want message_id and room_id", m)
	}
}

func TestPostThreadTitleTruncation(t *testing.T) {
	long := strings.Repeat("a", 100)
	c := &fakeClient{
		joined:   []apronws.Room{{ID: "r1"}},
		postID:   "msg-1",
		threadID: "t1",
	}
	if _, err := PostHandler(postDeps(t, c))(context.Background(),
		inv("post", []string{long + "\nrest"}, map[string]string{"room": "r1", "thread-of": "m2"}, nil), io.Discard); err != nil {
		t.Fatalf("PostHandler: %v", err)
	}
	if got := c.threads[0].Title; len([]rune(got)) != 80 {
		t.Fatalf("Title = %q (%d runes), want 80", got, len([]rune(got)))
	}
}

func TestPostReplyToAndThreadOfExclusive(t *testing.T) {
	c := &fakeClient{joined: []apronws.Room{{ID: "r1"}}}
	_, err := PostHandler(testDeps(c))(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1", "reply-to": "m1", "thread-of": "m2"}, nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %v, want a mutually exclusive error", err)
	}
}

func TestPostStdin(t *testing.T) {
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1"}},
		postID: "msg-1",
	}
	d := postDeps(t, c)
	d.Stdin = strings.NewReader("from stdin\n")
	if _, err := PostHandler(d)(context.Background(),
		inv("post", []string{"-"}, map[string]string{"room": "r1"}, nil), io.Discard); err != nil {
		t.Fatalf("PostHandler: %v", err)
	}
	if got := c.posts[0].Body.Text; got != "from stdin" {
		t.Fatalf("Body.Text = %q, want the trimmed stdin", got)
	}
}

func TestPostMissingToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()
	d := Deps{Env: func(string) string { return "" }, Store: s}
	_, err = PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "no apron token") {
		t.Fatalf("err = %v, want a missing token error", err)
	}
}

// TestPostRefusesWhenBreakerLatched: a latched breaker refuses the
// post before any network call; the typed denial text is the error.
func TestPostRefusesWhenBreakerLatched(t *testing.T) {
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1", Title: "General"}},
		postID: "msg-1",
	}
	d := postDeps(t, c)
	if err := d.Store.SetBreaker(context.Background(), store.Breaker{Latched: true, Reason: "server rate limit"}); err != nil {
		t.Fatalf("SetBreaker: %v", err)
	}
	_, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil {
		t.Fatal("want a refusal, got nil")
	}
	if !strings.Contains(err.Error(), "refused-breaker") {
		t.Fatalf("err = %v, want a refused-breaker denial", err)
	}
	if !strings.Contains(err.Error(), "server rate limit") {
		t.Fatalf("err = %v, want the breaker reason in the text", err)
	}
	if len(c.posts) != 0 {
		t.Fatalf("posts = %d, want 0 (no send while latched)", len(c.posts))
	}
	if len(c.joinedRooms) != 0 {
		t.Fatalf("joinedRooms = %v, want none (no send while latched)", c.joinedRooms)
	}
}

// TestPostConsumesRateToken: an allowed post consumes a rate-bucket
// token and persists the consumed state to the store.
func TestPostConsumesRateToken(t *testing.T) {
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

	if _, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard); err != nil {
		t.Fatalf("PostHandler: %v", err)
	}
	if len(c.posts) != 1 {
		t.Fatalf("posts = %d, want 1", len(c.posts))
	}
	tok, last, err := d.Store.RateState(context.Background())
	if err != nil {
		t.Fatalf("RateState: %v", err)
	}
	if tok != 4 {
		t.Errorf("tokens = %v, want 4 (one consumed from a full 5-bucket)", tok)
	}
	if !last.Equal(now) {
		t.Errorf("last = %v, want the injected now", last)
	}
}

// TestPostRefusesWhenRateExhausted: a pre-exhausted persisted bucket
// (zero tokens, set through the store's rate-state helper) refuses the
// post with the typed denial and a retry-after hint, and no send goes
// out.
func TestPostRefusesWhenRateExhausted(t *testing.T) {
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
	_, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil {
		t.Fatal("want a refusal, got nil")
	}
	if !strings.Contains(err.Error(), "refused-rate-limit") {
		t.Fatalf("err = %v, want a refused-rate-limit denial", err)
	}
	if !strings.Contains(err.Error(), "retry is safe after") {
		t.Fatalf("err = %v, want a retry-after hint", err)
	}
	if len(c.posts) != 0 {
		t.Fatalf("posts = %d, want 0 (no send while rate-limited)", len(c.posts))
	}
}

// TestPostSecondRefusedAtRateOne: with WHINNY_RATE_PER_MINUTE=1 the
// first post consumes the bucket's only token (persisted by the bucket's
// save hook) and the second is refused; only the first is sent.
func TestPostSecondRefusedAtRateOne(t *testing.T) {
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1", Title: "General"}},
		postID: "msg-1",
	}
	d := postDeps(t, c)
	d.Env = func(k string) string {
		if k == "WHINNY_RATE_PER_MINUTE" {
			return "1"
		}
		return ""
	}
	now := time.Unix(2_000_000, 0)
	d.Now = func() time.Time { return now }

	if _, err := PostHandler(d)(context.Background(),
		inv("post", []string{"first"}, map[string]string{"room": "r1"}, nil), io.Discard); err != nil {
		t.Fatalf("first post: %v", err)
	}
	_, err := PostHandler(d)(context.Background(),
		inv("post", []string{"second"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "refused-rate-limit") {
		t.Fatalf("second post err = %v, want a refused-rate-limit denial", err)
	}
	if len(c.posts) != 1 {
		t.Fatalf("posts = %d, want 1 (only the first)", len(c.posts))
	}
}

// TestPostInvalidRateEnv: a non-numeric WHINNY_RATE_PER_MINUTE is a
// usage error naming the variable, before any guard or send.
func TestPostInvalidRateEnv(t *testing.T) {
	c := &fakeClient{joined: []apronws.Room{{ID: "r1"}}}
	d := postDeps(t, c)
	d.Env = func(k string) string {
		if k == "WHINNY_RATE_PER_MINUTE" {
			return "abc"
		}
		return ""
	}
	_, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "WHINNY_RATE_PER_MINUTE") {
		t.Fatalf("err = %v, want an error naming WHINNY_RATE_PER_MINUTE", err)
	}
	e, ok := err.(*Error)
	if !ok || e.Class != ClassUsage {
		t.Fatalf("err = %v, want a usage-class error", err)
	}
	if len(c.posts) != 0 {
		t.Fatalf("posts = %d, want 0 (no send on a usage error)", len(c.posts))
	}
}

// TestPostRefusesWhenQuiet: a quiet gate in the future refuses the
// post with the typed quiet denial.
func TestPostRefusesWhenQuiet(t *testing.T) {
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
	_, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "refused-quiet") {
		t.Fatalf("err = %v, want a refused-quiet denial", err)
	}
	if len(c.posts) != 0 {
		t.Fatalf("posts = %d, want 0 (no send while quiet)", len(c.posts))
	}
}

// TestPostRefundsRateTokenWhenPostFails: a post that fails after the
// guard's take refunds the consumed token, so a second post is allowed.
func TestPostRefundsRateTokenWhenPostFails(t *testing.T) {
	c := &fakeClient{
		joined:  []apronws.Room{{ID: "r1", Title: "General"}},
		postID:  "msg-1",
		postErr: errors.New("send failed"),
	}
	d := postDeps(t, c)
	d.Env = func(k string) string {
		if k == "WHINNY_RATE_PER_MINUTE" {
			return "1"
		}
		return ""
	}
	now := time.Unix(2_000_000, 0)
	d.Now = func() time.Time { return now }

	// First post: the guard takes the only token, the send fails, and
	// the token is refunded.
	_, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil {
		t.Fatal("want a send failure, got nil")
	}
	tok, _, err := d.Store.RateState(context.Background())
	if err != nil {
		t.Fatalf("RateState: %v", err)
	}
	if tok != 1 {
		t.Fatalf("tokens = %v, want 1 (refunded after a failed send)", tok)
	}
	// Second post: the refunded token is available, so it succeeds.
	c.postErr = nil
	if _, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard); err != nil {
		t.Fatalf("second post: %v", err)
	}
}

// TestPostRefundsRateTokenWhenConnectFails: a post that fails to
// connect after the guard's take refunds the consumed token.
func TestPostRefundsRateTokenWhenConnectFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	d := Deps{
		Store: s,
		Env: func(k string) string {
			if k == "WHINNY_RATE_PER_MINUTE" {
				return "1"
			}
			return ""
		},
		Now: func() time.Time { return time.Unix(2_000_000, 0) },
		Connect: func(ctx context.Context, cfg Config) (RoomClient, error) {
			return nil, errors.New("connect failed")
		},
	}
	_, err = PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil {
		t.Fatal("want a connect failure, got nil")
	}
	tok, _, err := s.RateState(context.Background())
	if err != nil {
		t.Fatalf("RateState: %v", err)
	}
	if tok != 1 {
		t.Fatalf("tokens = %v, want 1 (refunded after a failed connect)", tok)
	}
}
