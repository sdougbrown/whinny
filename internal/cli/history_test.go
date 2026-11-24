package cli

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/sdougbrown/whinny/internal/apronws"
)

func cannedMessages() []apronws.Message {
	return []apronws.Message{
		{MessageID: "m1", LogID: 1, RoomID: "r1", From: apronws.From{UserID: "u1", Name: "alice"}, Body: &apronws.Body{Text: "hello"}},
		{MessageID: "m2", LogID: 2, RoomID: "r1", From: apronws.From{UserID: "u2", Name: "bob"}, Body: &apronws.Body{Text: "world"}},
		{MessageID: "m3", LogID: 3, RoomID: "r1", From: apronws.From{UserID: "u1", Name: "alice"}, Body: &apronws.Body{Text: "again"}},
	}
}

func historyDeps() (Deps, *fakeClient) {
	c := &fakeClient{
		joined:  []apronws.Room{{ID: "r1", Title: "General"}},
		history: apronws.HistoryResult{Messages: cannedMessages()},
	}
	return testDeps(c), c
}

func TestHistorySinceFilter(t *testing.T) {
	d, _ := historyDeps()
	res, err := HistoryHandler(d)(context.Background(),
		inv("history", nil, map[string]string{"room": "r1", "since": "m1"}, nil), io.Discard)
	if err != nil {
		t.Fatalf("HistoryHandler: %v", err)
	}
	if got, ok := res.(string); !ok || got != "bob: world\nalice: again" {
		t.Fatalf("result = %v, want the two messages after the cursor", res)
	}
}

func TestHistoryBeforeFilter(t *testing.T) {
	d, _ := historyDeps()
	res, err := HistoryHandler(d)(context.Background(),
		inv("history", nil, map[string]string{"room": "r1", "before": "m3"}, nil), io.Discard)
	if err != nil {
		t.Fatalf("HistoryHandler: %v", err)
	}
	if got, ok := res.(string); !ok || got != "alice: hello\nbob: world" {
		t.Fatalf("result = %v, want the two messages before the cursor", res)
	}
}

func TestHistoryCursorNotFound(t *testing.T) {
	d, _ := historyDeps()
	_, err := HistoryHandler(d)(context.Background(),
		inv("history", nil, map[string]string{"room": "r1", "since": "m9"}, nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not in fetched history") {
		t.Fatalf("err = %v, want a cursor-not-found error", err)
	}
}

func TestHistoryJSON(t *testing.T) {
	d, _ := historyDeps()
	res, err := HistoryHandler(d)(context.Background(),
		inv("history", nil, map[string]string{"room": "r1"}, map[string]bool{"json": true}), io.Discard)
	if err != nil {
		t.Fatalf("HistoryHandler: %v", err)
	}
	msgs, ok := res.([]historyMessage)
	if !ok || len(msgs) != 3 {
		t.Fatalf("result = %v, want a 3-message JSON array", res)
	}
	if msgs[0].MessageID != "m1" || msgs[0].Text != "hello" || msgs[0].From.Name != "alice" {
		t.Fatalf("first message = %+v, want m1/hello/alice", msgs[0])
	}
}

func TestHistoryLimitDefault(t *testing.T) {
	d, c := historyDeps()
	if _, err := HistoryHandler(d)(context.Background(),
		inv("history", nil, map[string]string{"room": "r1"}, nil), io.Discard); err != nil {
		t.Fatalf("HistoryHandler: %v", err)
	}
	if got := c.histories[0].Limit; got != 50 {
		t.Fatalf("Limit = %d, want the 50 default", got)
	}
}

func TestHistoryReactionsAttached(t *testing.T) {
	d, c := historyDeps()
	c.history.Reactions = []apronws.ReactionsRecord{
		{MessageID: "m1", LogID: 10, RoomID: "r1", Reactions: []apronws.ReactionSet{
			{From: apronws.From{UserID: "u2", Name: "bob"}, Emojis: []string{"👍"}},
		}},
	}
	res, err := HistoryHandler(d)(context.Background(),
		inv("history", nil, map[string]string{"room": "r1"}, map[string]bool{"json": true}), io.Discard)
	if err != nil {
		t.Fatalf("HistoryHandler: %v", err)
	}
	msgs := res.([]historyMessage)
	if len(msgs[0].Reactions) != 1 || msgs[0].Reactions[0].From.UserID != "u2" {
		t.Fatalf("m1 reactions = %+v, want bob's set", msgs[0].Reactions)
	}
	if len(msgs[1].Reactions) != 0 {
		t.Errorf("m2 reactions = %+v, want none", msgs[1].Reactions)
	}
	// Text mode: the reacting author and emoji ride the message line.
	res, err = HistoryHandler(d)(context.Background(),
		inv("history", nil, map[string]string{"room": "r1"}, nil), io.Discard)
	if err != nil {
		t.Fatalf("HistoryHandler text: %v", err)
	}
	text := res.(string)
	if !strings.Contains(text, "bob reacted 👍") {
		t.Errorf("text = %q, want a bob reacted line", text)
	}
}

// TestFoldReactionsTable pins the fold's replace/clear/re-add model per
// §4.7: each record carries one author's complete set at that log
// position; the last record naming an author wins.
func TestFoldReactionsTable(t *testing.T) {
	msgs := []apronws.Message{
		{MessageID: "m1", RoomID: "r1"},
		{MessageID: "m2", RoomID: "r1"},
	}
	from := func(id string) apronws.From { return apronws.From{UserID: id} }
	rec := func(id string, sets ...apronws.ReactionSet) apronws.ReactionsRecord {
		return apronws.ReactionsRecord{MessageID: id, RoomID: "r1", Reactions: sets}
	}
	set := func(userID string, emojis ...string) apronws.ReactionSet {
		return apronws.ReactionSet{From: from(userID), Emojis: emojis}
	}
	tests := []struct {
		name    string
		records []apronws.ReactionsRecord
		want    map[string][]apronws.ReactionSet
	}{
		{
			name:    "later set by same author replaces",
			records: []apronws.ReactionsRecord{rec("m1", set("A", "👍")), rec("m1", set("A", "🎉"))},
			want:    map[string][]apronws.ReactionSet{"m1": {set("A", "🎉")}},
		},
		{
			name:    "clear removes the author and the message key",
			records: []apronws.ReactionsRecord{rec("m1", set("A", "👍")), rec("m1", set("A"))},
			want:    map[string][]apronws.ReactionSet{},
		},
		{
			name:    "re-add after clear",
			records: []apronws.ReactionsRecord{rec("m1", set("A", "👍")), rec("m1", set("A")), rec("m1", set("A", "🎉"))},
			want:    map[string][]apronws.ReactionSet{"m1": {set("A", "🎉")}},
		},
		{
			name: "one author clears while another replaces",
			records: []apronws.ReactionsRecord{
				rec("m1", set("A", "👍"), set("B", "🎉")),
				rec("m1", set("A")),
				rec("m1", set("B", "🚀")),
			},
			want: map[string][]apronws.ReactionSet{"m1": {set("B", "🚀")}},
		},
		{
			name:    "clear for a never-seen author is a no-op",
			records: []apronws.ReactionsRecord{rec("m1", set("X"))},
			want:    map[string][]apronws.ReactionSet{},
		},
		{
			name:    "record for a message outside the window is dropped",
			records: []apronws.ReactionsRecord{rec("m9", set("A", "👍"))},
			want:    map[string][]apronws.ReactionSet{},
		},
		{
			name:    "per-message state stays separate",
			records: []apronws.ReactionsRecord{rec("m1", set("A", "👍")), rec("m2", set("B", "🎉"))},
			want:    map[string][]apronws.ReactionSet{"m1": {set("A", "👍")}, "m2": {set("B", "🎉")}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := foldReactions(msgs, tt.records)
			if len(got) != len(tt.want) {
				t.Fatalf("fold = %+v, want %+v", got, tt.want)
			}
			for id, wantSets := range tt.want {
				gotSets := got[id]
				if len(gotSets) != len(wantSets) {
					t.Fatalf("fold[%s] = %+v, want %+v", id, gotSets, wantSets)
				}
				for i, w := range wantSets {
					if gotSets[i].From.UserID != w.From.UserID || strings.Join(gotSets[i].Emojis, ",") != strings.Join(w.Emojis, ",") {
						t.Fatalf("fold[%s][%d] = %+v, want %+v", id, i, gotSets[i], w)
					}
				}
			}
		})
	}
}
