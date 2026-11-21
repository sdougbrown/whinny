package wake

import (
	"context"
	"errors"
	"testing"
	"time"
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

var testNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

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
			Msg:     Msg{From: human, ReplyTo: "m100"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindEdge, ReplyTo: "m100", Note: "edge note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch || d.Reason != "edge note" {
			t.Errorf("d = %+v, want wake watch 'edge note'", d)
		}
	})
	t.Run("pattern hit on text", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: human, Text: "the DEPLOY is done"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "deploy", Note: "deploy note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch || d.Reason != "deploy note" {
			t.Errorf("d = %+v, want wake watch 'deploy note'", d)
		}
	})
	t.Run("pattern hit on embed title", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: human, Text: "see attached", Embeds: []Embed{{Title: "DEPLOY report"}}},
			Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "deploy", Note: "embed note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Errorf("d = %+v, want wake watch", d)
		}
	})
	t.Run("pattern hit on embed caption", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: human, Text: "see attached", Embeds: []Embed{{Caption: "the deploy log"}}},
			Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "deploy", Note: "caption note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Errorf("d = %+v, want wake watch", d)
		}
	})
	t.Run("thread match one hop", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: human, ReplyTo: "thread-root"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindThread, ThreadID: "thread-root", Note: "thread note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch || d.Reason != "thread note" {
			t.Errorf("d = %+v, want wake watch 'thread note'", d)
		}
	})
	t.Run("subscription hit on human post", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: human, ReplyTo: "thread-root"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindSubscription, ThreadID: "thread-root", Note: "sub note"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Errorf("d = %+v, want wake watch", d)
		}
	})
	t.Run("subscription excludes owner own post", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: From{UserID: "bot1"}, ReplyTo: "thread-root"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindSubscription, ThreadID: "thread-root", Note: "sub note"}},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (owner's own post)")
		}
	})
	t.Run("from filter mismatch", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: human, Text: "deploy"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "deploy", FromUser: "otheruser", Note: "filtered"}},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (from filter mismatch)")
		}
	})
	t.Run("from filter match", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: human, Text: "deploy"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "deploy", FromUser: "human1", Note: "filtered"}},
		}
		d := Evaluate(context.Background(), in)
		if !d.Wake || d.Kind != KindWatch {
			t.Errorf("d = %+v, want wake watch", d)
		}
	})
	t.Run("once watch removed", func(t *testing.T) {
		in := In{
			Self:    self,
			Msg:     Msg{From: human, Text: "deploy"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "deploy", Once: true, Note: "once note"}},
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
			Msg:     Msg{From: human, Text: "deploy"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "deploy", Once: false, Note: "persist note"}},
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
			Msg:     Msg{From: human, Text: "deploy"},
			Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "(", Note: "bad"}},
		}
		d := Evaluate(context.Background(), in)
		if d.Wake {
			t.Errorf("d.Wake = true, want false (uncompileable pattern)")
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
		gate.Record(testNow)
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
			gate.Record(testNow.Add(time.Duration(i) * time.Minute))
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
	t.Run("classifier false no wake no record", func(t *testing.T) {
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
		// State not recorded: flip the classifier to true; the immediate
		// follow-up is still allowed by the valve and wakes.
		classifier.directed = true
		in2 := in
		in2.Clock = fixedClock{t: testNow.Add(10 * time.Second)}
		d2 := Evaluate(context.Background(), in2)
		if !d2.Wake || d2.Kind != KindAmbient {
			t.Errorf("d2 = %+v, want wake ambient (state not recorded)", d2)
		}
	})
}

func TestOrderingMentionBeatsWatch(t *testing.T) {
	self := testSelf()
	human := humanFrom()
	in := In{
		Self:    self,
		Msg:     Msg{From: human, Text: "squire, deploy"},
		Watches: []Watch{{ID: "w1", Kind: WatchKindPattern, Pattern: "deploy", Once: true, Note: "watch note"}},
	}
	d := Evaluate(context.Background(), in)
	if d.Kind != KindMention {
		t.Errorf("d.Kind = %q, want mention (mention beats watch)", d.Kind)
	}
	if !d.Wake {
		t.Errorf("d.Wake = false, want true")
	}
	if len(d.RemoveWatchIDs) != 0 {
		t.Errorf("RemoveWatchIDs = %v, want empty (watch not consumed)", d.RemoveWatchIDs)
	}
}

func TestExpiryDecision(t *testing.T) {
	w := Watch{ID: "w1", Note: "deploy note"}
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
		if !g.Allow(testNow) {
			t.Errorf("Allow = false, want true (fresh)")
		}
	})
	t.Run("spacing blocks then allows at boundary", func(t *testing.T) {
		g := NewAmbientGate(5*time.Minute, 10)
		g.Record(testNow)
		if g.Allow(testNow.Add(4 * time.Minute)) {
			t.Errorf("Allow = true, want false (within spacing)")
		}
		if !g.Allow(testNow.Add(5 * time.Minute)) {
			t.Errorf("Allow = false, want true (at spacing boundary)")
		}
	})
	t.Run("cap blocks at N", func(t *testing.T) {
		g := NewAmbientGate(0, 2)
		g.Record(testNow)
		g.Record(testNow.Add(time.Minute))
		if g.Allow(testNow.Add(2 * time.Minute)) {
			t.Errorf("Allow = true, want false (cap reached)")
		}
	})
	t.Run("cap window expires", func(t *testing.T) {
		g := NewAmbientGate(0, 1)
		g.Record(testNow)
		if !g.Allow(testNow.Add(time.Hour + time.Second)) {
			t.Errorf("Allow = false, want true (window expired)")
		}
	})
}
