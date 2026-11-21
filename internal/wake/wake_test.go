package wake

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/watch"
)

// fixedClock is a deterministic Clock.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// stubClassifier is a counting Classifier stub.
type stubClassifier struct {
	calls    int
	directed bool
	err      error
}

func (c *stubClassifier) IsDirected(ctx context.Context, msg Msg, self Self) (bool, error) {
	c.calls++
	return c.directed, c.err
}

// historyOf builds a HistoryLookup from a message_id -> user_id map.
func historyOf(m map[string]string) HistoryLookup {
	return func(ctx context.Context, messageID string) (string, bool, error) {
		u, ok := m[messageID]
		return u, ok, nil
	}
}

var (
	testNow   = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	trueBool  = true
	falseBool = false
)

func testSelf() Self {
	return Self{UserID: "bot1", Name: "squire", Aliases: []string{"sq"}}
}

func humanFrom() From { return From{UserID: "human1", Name: "human"} }

func TestMention(t *testing.T) {
	self := testSelf()
	cases := []struct {
		name string
		msg  Msg
		want bool
	}{
		{"name anywhere", Msg{Text: "hello squire how are you"}, true},
		{"@name", Msg{Text: "@squire ping"}, true},
		{"prefix colon", Msg{Text: "squire: do the thing"}, true},
		{"prefix comma", Msg{Text: "squire, do the thing"}, true},
		{"case-insensitive", Msg{Text: "Hello SQUIRE"}, true},
		{"alias", Msg{Text: "hey sq"}, true},
		{"@alias", Msg{Text: "@sq hi"}, true},
		{"mentions array", Msg{Mentions: []string{"bot1"}}, true},
		{"non-mention", Msg{Text: "hello world"}, false},
		{"partial name", Msg{Text: "squirebot is here"}, false},
		{"other user id", Msg{Mentions: []string{"someoneelse"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := In{Self: self, Msg: tc.msg}
			d := Evaluate(context.Background(), in)
			if d.Wake != tc.want {
				t.Errorf("Wake = %v, want %v", d.Wake, tc.want)
			}
			if tc.want && d.Kind != KindMention {
				t.Errorf("Kind = %q, want mention", d.Kind)
			}
		})
	}
}

func TestMentionReason(t *testing.T) {
	self := testSelf()
	in := In{Self: self, Msg: Msg{Text: "hello squire"}}
	d := Evaluate(context.Background(), in)
	if d.Reason != `mentioned "squire"` {
		t.Errorf("Reason = %q, want %q", d.Reason, `mentioned "squire"`)
	}
}

func TestReply(t *testing.T) {
	self := testSelf()

	t.Run("hit", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: humanFrom(), ReplyTo: "m100"},
			History: historyOf(map[string]string{"m100": "bot1"}),
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindReply {
			t.Errorf("d = %+v, want wake reply", d)
		}
		if d.Reason != "reply to my message" {
			t.Errorf("Reason = %q, want 'reply to my message'", d.Reason)
		}
	})
	t.Run("miss other author", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: humanFrom(), ReplyTo: "m100"},
			History: historyOf(map[string]string{"m100": "someoneelse"}),
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (other author)")
		}
	})
	t.Run("miss not found", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: humanFrom(), ReplyTo: "m404"},
			History: historyOf(map[string]string{}),
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (not found)")
		}
	})
}

func TestWatches(t *testing.T) {
	self := testSelf()
	human := humanFrom()

	t.Run("edge hit", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, ReplyTo: "m100"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindEdge, ReplyTo: "m100", Note: "edge note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch || d.Reason != "edge note" {
			t.Errorf("d = %+v, want wake watch 'edge note'", d)
		}
	})
	t.Run("pattern hit on text", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "the DEPLOY is done"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Note: "deploy note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch || d.Reason != "deploy note" {
			t.Errorf("d = %+v, want wake watch 'deploy note'", d)
		}
	})
	t.Run("pattern hit on embed title", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "see attached", Embeds: []Embed{{Title: "DEPLOY report"}}},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Note: "embed note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Errorf("d = %+v, want wake watch", d)
		}
	})
	t.Run("pattern hit on embed caption", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "see attached", Embeds: []Embed{{Caption: "the deploy log"}}},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Note: "caption note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Errorf("d = %+v, want wake watch", d)
		}
	})
	t.Run("thread match one hop", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, ReplyTo: "thread-root"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindThread, ThreadID: "thread-root", Note: "thread note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch || d.Reason != "thread note" {
			t.Errorf("d = %+v, want wake watch 'thread note'", d)
		}
	})
	t.Run("subscription hit on human post", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, ReplyTo: "thread-root"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindSubscription, ThreadID: "thread-root", Note: "sub note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Errorf("d = %+v, want wake watch", d)
		}
	})
	t.Run("subscription excludes owner own post", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: From{UserID: "bot1"}, ReplyTo: "thread-root"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindSubscription, ThreadID: "thread-root", Note: "sub note"}},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (owner's own post)")
		}
	})
	t.Run("from filter mismatch", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "deploy"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", FromUser: "otheruser", Note: "filtered"}},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (from filter mismatch)")
		}
	})
	t.Run("from filter match", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "deploy"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", FromUser: "human1", Note: "filtered"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Errorf("d = %+v, want wake watch", d)
		}
	})
	t.Run("once watch removed", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "deploy"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Once: &trueBool, Note: "once note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Fatalf("d = %+v, want wake watch", d)
		}
		if len(d.RemoveWatchIDs) != 1 || d.RemoveWatchIDs[0] != "w1" {
			t.Errorf("RemoveWatchIDs = %v, want [w1]", d.RemoveWatchIDs)
		}
	})
	t.Run("persistent watch stays", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "deploy"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Once: &falseBool, Note: "persist note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Fatalf("d = %+v, want wake watch", d)
		}
		if len(d.RemoveWatchIDs) != 0 {
			t.Errorf("RemoveWatchIDs = %v, want empty", d.RemoveWatchIDs)
		}
	})
	t.Run("uncompileable pattern is non-match", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "deploy"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "(", Note: "bad"}},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (uncompileable pattern)")
		}
	})
	t.Run("two once-watches both fire", func(t *testing.T) {
		in := In{
			Self: self,
			Msg:  Msg{RoomID: "room1", From: human, Text: "deploy"},
			Watches: []watch.Watch{
				{ID: "w1", Owner: "agent-a", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Once: &trueBool, Note: "first"},
				{ID: "w2", Owner: "agent-b", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Once: &trueBool, Note: "second"},
			},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Fatalf("d = %+v, want wake watch", d)
		}
		if len(d.Watches) != 2 {
			t.Fatalf("Watches len = %d, want 2", len(d.Watches))
		}
		if d.Watches[0].ID != "w1" || d.Watches[0].Owner != "agent-a" || d.Watches[0].Note != "first" {
			t.Errorf("Watches[0] = %+v, want w1/agent-a/first", d.Watches[0])
		}
		if d.Watches[1].ID != "w2" || d.Watches[1].Owner != "agent-b" || d.Watches[1].Note != "second" {
			t.Errorf("Watches[1] = %+v, want w2/agent-b/second", d.Watches[1])
		}
		if len(d.RemoveWatchIDs) != 2 || d.RemoveWatchIDs[0] != "w1" || d.RemoveWatchIDs[1] != "w2" {
			t.Errorf("RemoveWatchIDs = %v, want [w1 w2]", d.RemoveWatchIDs)
		}
	})
	t.Run("mixed once and persistent", func(t *testing.T) {
		in := In{
			Self: self,
			Msg:  Msg{RoomID: "room1", From: human, Text: "deploy"},
			Watches: []watch.Watch{
				{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Once: &trueBool, Note: "once"},
				{ID: "w2", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Once: &falseBool, Note: "persist"},
			},
		}
		d := Evaluate(context.Background(), in)
		if len(d.Watches) != 2 {
			t.Fatalf("Watches len = %d, want 2", len(d.Watches))
		}
		if len(d.RemoveWatchIDs) != 1 || d.RemoveWatchIDs[0] != "w1" {
			t.Errorf("RemoveWatchIDs = %v, want [w1] (only the once-hit)", d.RemoveWatchIDs)
		}
	})
	t.Run("empty room never matches", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "deploy"},
			Watches: []watch.Watch{{ID: "w1", Kind: watch.KindPattern, Pattern: "deploy", Note: "no room"}},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (empty room never matches)")
		}
	})
	for _, tc := range []struct {
		name  string
		kind  watch.Kind
		watch watch.Watch
	}{
		{"thread watch non-reply", watch.KindThread, watch.Watch{ID: "w1", RoomID: "room1", Kind: watch.KindThread, ThreadID: "thread-root", Note: "thread note"}},
		{"edge watch non-reply", watch.KindEdge, watch.Watch{ID: "w1", RoomID: "room1", Kind: watch.KindEdge, ReplyTo: "m100", Note: "edge note"}},
		{"subscription watch non-reply", watch.KindSubscription, watch.Watch{ID: "w1", RoomID: "room1", Kind: watch.KindSubscription, ThreadID: "thread-root", Note: "sub note"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := In{
				Self:    self,
				Msg:     Msg{RoomID: "room1", From: human, Text: "hello"},
				Watches: []watch.Watch{tc.watch},
			}
			d := Evaluate(context.Background(), in)
			if d.Wake {
				t.Errorf("d.Wake = true, want false (%s, non-reply message)", tc.name)
			}
		})
	}
	t.Run("unknown kind never matches", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{RoomID: "room1", From: human, Text: "deploy", ReplyTo: "m100"},
			Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: "bogus", Pattern: "deploy", ReplyTo: "m100", ThreadID: "m100", Note: "bogus"}},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (unknown kind)")
		}
	})
}

func TestFirebreak(t *testing.T) {
	self := testSelf()
	classifier := &stubClassifier{directed: true}
	in := In{
		Self:       self,
		Msg:        Msg{From: From{UserID: "bot2", Name: "otherbot"}, Text: "just a bot message"},
		Peers:      Peers{"bot2": {Name: "otherbot", IsBot: true}},
		Classifier: classifier,
		Gate:       NewAmbientGate(time.Minute, 10),
		Clock:      fixedClock{t: testNow},
	}
	d := Evaluate(context.Background(), in)
	if d.Wake {
		t.Errorf("d.Wake = true, want false (bot firebreak)")
	}
	if classifier.calls != 0 {
		t.Errorf("classifier.calls = %d, want 0 (not called)", classifier.calls)
	}
}

func TestBotWatchMatch(t *testing.T) {
	self := testSelf()
	classifier := &stubClassifier{directed: true}
	in := In{
		Self:       self,
		Msg:        Msg{RoomID: "room1", From: From{UserID: "bot2", Name: "otherbot"}, Text: "the DEPLOY is done"},
		Peers:      Peers{"bot2": {Name: "otherbot", IsBot: true}},
		Watches:    []watch.Watch{{ID: "w1", Owner: "agent-a", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Note: "bot watch"}},
		Classifier: classifier,
		Gate:       NewAmbientGate(time.Minute, 10),
		Clock:      fixedClock{t: testNow},
	}
	d := Evaluate(context.Background(), in)
	if !d.Wake || d.Kind != KindWatch {
		t.Errorf("d = %+v, want wake watch (bot watch match)", d)
	}
	if len(d.Watches) != 1 || d.Watches[0].Owner != "agent-a" {
		t.Errorf("Watches = %v, want one hit owned by agent-a", d.Watches)
	}
	if classifier.calls != 0 {
		t.Errorf("classifier.calls = %d, want 0 (bots never reach the ambient gate)", classifier.calls)
	}
}

func TestAmbient(t *testing.T) {
	self := testSelf()
	human := humanFrom()

	t.Run("classifier true wakes", func(t *testing.T) {
		classifier := &stubClassifier{directed: true}
		in := In{
			Self:       self,
			Msg:        Msg{From: human, Text: "hello"},
			Classifier: classifier,
			Gate:       NewAmbientGate(time.Minute, 10),
			Clock:      fixedClock{t: testNow},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindAmbient {
			t.Errorf("d = %+v, want wake ambient", d)
		}
		if classifier.calls != 1 {
			t.Errorf("classifier.calls = %d, want 1", classifier.calls)
		}
	})
	t.Run("classifier error fails open", func(t *testing.T) {
		classifier := &stubClassifier{directed: false, err: errors.New("boom")}
		in := In{
			Self:       self,
			Msg:        Msg{From: human, Text: "hello"},
			Classifier: classifier,
			Gate:       NewAmbientGate(time.Minute, 10),
			Clock:      fixedClock{t: testNow},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindAmbient {
			t.Errorf("d = %+v, want wake ambient (fail-open)", d)
		}
	})
	t.Run("valve spacing blocks", func(t *testing.T) {
		classifier := &stubClassifier{directed: true}
		gate := NewAmbientGate(5*time.Minute, 10)
		if !gate.TryReserve(testNow) {
			t.Fatal("pre-reserve failed")
		}
		in := In{
			Self:       self,
			Msg:        Msg{From: human, Text: "hello"},
			Classifier: classifier,
			Gate:       gate,
			Clock:      fixedClock{t: testNow.Add(10 * time.Second)},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (spacing valve)")
		}
		if classifier.calls != 0 {
			t.Errorf("classifier.calls = %d, want 0 (valve blocks before classifier)", classifier.calls)
		}
	})
	t.Run("hourly cap blocks N+1th", func(t *testing.T) {
		const maxPerHour = 3
		classifier := &stubClassifier{directed: true}
		gate := NewAmbientGate(0, maxPerHour)
		for i := 0; i < maxPerHour; i++ {
			if !gate.TryReserve(testNow.Add(time.Duration(i) * time.Minute)) {
				t.Fatalf("pre-reserve %d failed", i)
			}
		}
		in := In{
			Self:       self,
			Msg:        Msg{From: human, Text: "hello"},
			Classifier: classifier,
			Gate:       gate,
			Clock:      fixedClock{t: testNow.Add(10 * time.Minute)},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (hourly cap)")
		}
		if classifier.calls != 0 {
			t.Errorf("classifier.calls = %d, want 0 (cap blocks before classifier)", classifier.calls)
		}
	})
	t.Run("classifier false no wake reservation released", func(t *testing.T) {
		classifier := &stubClassifier{directed: false}
		gate := NewAmbientGate(5*time.Minute, 10)
		in := In{
			Self:       self,
			Msg:        Msg{From: human, Text: "hello"},
			Classifier: classifier,
			Gate:       gate,
			Clock:      fixedClock{t: testNow},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (classifier false)")
		}
		// The reservation was released: flip the classifier to true; the
		// immediate follow-up is still allowed by the valve and wakes.
		classifier.directed = true
		in2 := in
		in2.Clock = fixedClock{t: testNow.Add(10 * time.Second)}
		d2 := Evaluate(context.Background(), in2)
		if !d2.Wake || d2.Kind != KindAmbient {
			t.Errorf("d2 = %+v, want wake ambient (reservation released)", d2)
		}
	})
	t.Run("nil classifier no ambient wake", func(t *testing.T) {
		in := In{
			Self:  self,
			Msg:   Msg{From: human, Text: "hello"},
			Gate:  NewAmbientGate(time.Minute, 10),
			Clock: fixedClock{t: testNow},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (nil classifier)")
		}
	})
}

func TestOrderingMentionBeatsWatch(t *testing.T) {
	self := testSelf()
	human := humanFrom()
	in := In{
		Self:    self,
		Msg:     Msg{From: human, Text: "squire, deploy"},
		Watches: []watch.Watch{{ID: "w1", RoomID: "room1", Kind: watch.KindPattern, Pattern: "deploy", Once: &trueBool, Note: "watch note"}},
	}
	d := Evaluate(context.Background(), in)
	if d.Kind != KindMention {
		t.Errorf("d.Kind = %q, want mention (mention beats watch)", d.Kind)
	}
	if !d.Wake {
		t.Errorf("d.Wake = false, want true")
	}
	if len(d.RemoveWatchIDs) != 0 {
		t.Errorf("RemoveWatchIDs = %v, want empty (mention short-circuits before watch matching)", d.RemoveWatchIDs)
	}
}

func TestExpiryDecision(t *testing.T) {
	w := watch.Watch{ID: "w1", Note: "deploy note"}
	d := ExpiryDecision(w)
	if !d.Wake || d.Kind != KindWatch {
		t.Errorf("d = %+v, want wake watch", d)
	}
	if d.Reason != "no match within ttl: deploy note" {
		t.Errorf("d.Reason = %q, want 'no match within ttl: deploy note'", d.Reason)
	}
}

func TestAmbientGateValves(t *testing.T) {
	t.Run("fresh gate allows", func(t *testing.T) {
		g := NewAmbientGate(5*time.Minute, 10)
		if !g.TryReserve(testNow) {
			t.Errorf("TryReserve = false, want true (fresh)")
		}
	})
	t.Run("spacing blocks then allows at boundary", func(t *testing.T) {
		g := NewAmbientGate(5*time.Minute, 10)
		if !g.TryReserve(testNow) {
			t.Fatal("first TryReserve = false, want true")
		}
		if g.TryReserve(testNow.Add(4 * time.Minute)) {
			t.Errorf("TryReserve = true, want false (within spacing)")
		}
		if !g.TryReserve(testNow.Add(5 * time.Minute)) {
			t.Errorf("TryReserve = false, want true (at spacing boundary)")
		}
	})
	t.Run("cap blocks at N", func(t *testing.T) {
		g := NewAmbientGate(0, 2)
		if !g.TryReserve(testNow) {
			t.Fatal("first TryReserve = false, want true")
		}
		if !g.TryReserve(testNow.Add(time.Minute)) {
			t.Fatal("second TryReserve = false, want true")
		}
		if g.TryReserve(testNow.Add(2 * time.Minute)) {
			t.Errorf("TryReserve = true, want false (cap reached)")
		}
	})
	t.Run("cap window expires", func(t *testing.T) {
		g := NewAmbientGate(0, 1)
		if !g.TryReserve(testNow) {
			t.Fatal("TryReserve = false, want true")
		}
		if !g.TryReserve(testNow.Add(time.Hour + time.Second)) {
			t.Errorf("TryReserve = false, want true (window expired)")
		}
	})
	t.Run("negative cap denies all", func(t *testing.T) {
		g := NewAmbientGate(0, -1)
		if g.TryReserve(testNow) {
			t.Errorf("TryReserve = true, want false (negative cap)")
		}
	})
}

// TestAmbientGateConcurrent: concurrent TryReserves never exceed the
// valves.
func TestAmbientGateConcurrent(t *testing.T) {
	const maxPerHour = 3
	g := NewAmbientGate(0, maxPerHour)
	var wg sync.WaitGroup
	var allowed int64
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g.TryReserve(testNow) {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()
	if allowed > maxPerHour {
		t.Errorf("allowed = %d, want <= %d", allowed, maxPerHour)
	}
}

// TestAmbientGateReleaseRestores: a released reservation frees both the
// spacing and the cap for the next TryReserve.
func TestAmbientGateReleaseRestores(t *testing.T) {
	g := NewAmbientGate(0, 1)
	if !g.TryReserve(testNow) {
		t.Fatal("first TryReserve = false, want true")
	}
	g.Release(testNow)
	if !g.TryReserve(testNow) {
		t.Fatal("TryReserve after Release = false, want true (state restored)")
	}
}
