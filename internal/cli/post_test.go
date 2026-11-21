package cli

import (
	"context"
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

func TestPostBadFormat(t *testing.T) {
	c := &fakeClient{joined: []apronws.Room{{ID: "r1"}}}
	_, err := PostHandler(testDeps(c))(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1", "format": "html"}, nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "markdown or plain") {
		t.Fatalf("err = %v, want a format error", err)
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
