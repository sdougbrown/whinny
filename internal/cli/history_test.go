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
