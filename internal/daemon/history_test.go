package daemon

import (
	"context"
	"fmt"
	"testing"

	"github.com/sdougbrown/whinny/internal/wake"
)

// TestHistoryCacheFeedBounded: feeding more than maxSeen entries keeps
// the cache at maxSeen and evicts the oldest (FIFO): a fed-then-evicted
// entry no longer resolves, while the newest still does.
func TestHistoryCacheFeedBounded(t *testing.T) {
	h := newHistoryCache(newFakeLink())
	total := maxSeen + 10
	for i := 0; i < total; i++ {
		h.feed(fmt.Sprintf("m%d", i), "r1", fmt.Sprintf("a%d", i))
	}
	h.mu.Lock()
	n := len(h.seen)
	h.mu.Unlock()
	if n != maxSeen {
		t.Fatalf("cache size = %d, want %d", n, maxSeen)
	}
	ctx := context.Background()
	// The 10 oldest entries were evicted: they no longer resolve.
	for i := 0; i < 10; i++ {
		if _, ok, _ := h.lookup(ctx, "r1", fmt.Sprintf("m%d", i)); ok {
			t.Errorf("evicted entry m%d still resolves", i)
		}
	}
	// The newest entry still resolves with its author.
	author, ok, err := h.lookup(ctx, "r1", fmt.Sprintf("m%d", total-1))
	if err != nil || !ok || author != fmt.Sprintf("a%d", total-1) {
		t.Errorf("newest entry = %q, %v, %v; want a%d, true, nil", author, ok, err, total-1)
	}
}

// TestHistoryCacheLookupBounded: the History fallback loop (a miss
// target fetching a room whose history exceeds maxSeen) also keeps the
// cache at maxSeen, including the cached miss.
func TestHistoryCacheLookupBounded(t *testing.T) {
	fl := newFakeLink()
	total := maxSeen + 10
	var evs []LinkEvent
	for i := 0; i < total; i++ {
		evs = append(evs, LinkEvent{
			Kind:   KindMessage,
			RoomID: "r1",
			Msg:    wake.Msg{MessageID: fmt.Sprintf("m%d", i), RoomID: "r1", From: wake.From{UserID: fmt.Sprintf("a%d", i)}},
			Cursor: int64(i + 1),
		})
	}
	fl.setHistory(evs)
	h := newHistoryCache(fl)
	ctx := context.Background()
	if _, ok, err := h.lookup(ctx, "r1", "target"); err != nil || ok {
		t.Fatalf("lookup = %v, %v; want a miss", ok, err)
	}
	h.mu.Lock()
	n := len(h.seen)
	h.mu.Unlock()
	if n != maxSeen {
		t.Fatalf("cache size = %d, want %d", n, maxSeen)
	}
	// The cached miss holds: a second lookup does not re-fetch.
	if _, ok, err := h.lookup(ctx, "r1", "target"); err != nil || ok {
		t.Fatalf("second lookup = %v, %v; want the cached miss", ok, err)
	}
}
