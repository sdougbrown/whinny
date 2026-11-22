package daemon

import (
	"context"
	"sync"
)

// maxSeen bounds the record-stream author cache: the most recent 1024
// message ids are kept, and the oldest is evicted when the bound is
// exceeded.
const maxSeen = 1024

// historyCache resolves reply-target authors, preferring the event
// stream (fed by processEvent as messages stream past) and falling back
// to the link's History only on a miss. A miss is cached as
// not-found, so a reply storm costs one lookup. It satisfies
// wake.HistoryLookup.
//
// The cache is keyed by room + message id: apron message ids are unique
// across rooms, but Slack's are per-channel ts values, so the room is
// part of the key.
type historyCache struct {
	link  Link
	mu    sync.Mutex
	seen  map[string]histEntry
	order []string // FIFO order for eviction; entries are room\x00message keys
}

type histEntry struct {
	author string
	room   string
	ok     bool
}

func newHistoryCache(link Link) *historyCache {
	return &historyCache{link: link, seen: make(map[string]histEntry)}
}

// feed records the author of a message as it streams past, so a later
// reply to it resolves without a History call. It overwrites a cached
// not-found (a message can be seen in the stream after a History miss).
func (h *historyCache) feed(messageID, roomID, author string) {
	if messageID == "" {
		return
	}
	key := roomID + "\x00" + messageID
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.seen[key]; !ok {
		h.order = append(h.order, key)
	}
	h.seen[key] = histEntry{author: author, room: roomID, ok: true}
	for len(h.order) > maxSeen {
		delete(h.seen, h.order[0])
		h.order = h.order[1:]
	}
}

// lookup returns the author of messageID in roomID, preferring the
// event-stream cache and falling back to the link's History (scoped to
// the room) on a miss. ok is false when neither the cache nor the
// room's fetched window contains it.
func (h *historyCache) lookup(ctx context.Context, roomID, messageID string) (string, bool, error) {
	key := roomID + "\x00" + messageID
	h.mu.Lock()
	if e, ok := h.seen[key]; ok {
		h.mu.Unlock()
		return e.author, e.ok, nil
	}
	h.mu.Unlock()
	events, err := h.link.History(ctx, roomID, 0, HistoryLookupLimit)
	if err != nil {
		// An error is not cached: the next reply gets a fresh chance.
		return "", false, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ev := range events {
		m := ev.Msg
		key := m.RoomID + "\x00" + m.MessageID
		if _, seen := h.seen[key]; !seen {
			h.order = append(h.order, key)
		}
		h.seen[key] = histEntry{author: m.From.UserID, room: m.RoomID, ok: true}
	}
	// A target outside the fetched window is a miss, cached as such.
	if e, ok := h.seen[key]; ok {
		return e.author, e.ok, nil
	}
	if _, seen := h.seen[key]; !seen {
		h.order = append(h.order, key)
		h.seen[key] = histEntry{}
	}
	for len(h.order) > maxSeen {
		delete(h.seen, h.order[0])
		h.order = h.order[1:]
	}
	return "", false, nil
}
