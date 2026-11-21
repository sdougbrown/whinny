package cli

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/sdougbrown/whinny/internal/apronws"
)

func TestPostResolvesRoomAndPosts(t *testing.T) {
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1", Title: "General"}},
		postID: "msg-1",
	}
	res, err := PostHandler(testDeps(c))(context.Background(),
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
	res, err := PostHandler(testDeps(c))(context.Background(),
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
	if _, err := PostHandler(testDeps(c))(context.Background(),
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
	d := testDeps(c)
	d.Stdin = strings.NewReader("from stdin\n")
	if _, err := PostHandler(d)(context.Background(),
		inv("post", []string{"-"}, map[string]string{"room": "r1"}, nil), io.Discard); err != nil {
		t.Fatalf("PostHandler: %v", err)
	}
	if got := c.posts[0].Body.Text; got != "from stdin" {
		t.Fatalf("Body.Text = %q, want the trimmed stdin", got)
	}
}

func TestPostNoSubscribe(t *testing.T) {
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1"}},
		postID: "msg-1",
	}
	if _, err := PostHandler(testDeps(c))(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, map[string]bool{"no-subscribe": true}), io.Discard); err != nil {
		t.Fatalf("PostHandler: %v", err)
	}
	if len(c.joinedRooms) != 0 {
		t.Fatalf("joinedRooms = %v, want none", c.joinedRooms)
	}
}

func TestPostMissingToken(t *testing.T) {
	d := Deps{Env: func(string) string { return "" }}
	_, err := PostHandler(d)(context.Background(),
		inv("post", []string{"hello"}, map[string]string{"room": "r1"}, nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "no apron token") {
		t.Fatalf("err = %v, want a missing token error", err)
	}
}
