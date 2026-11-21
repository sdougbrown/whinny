package daemon

import (
	"context"
	"sync"

	"github.com/sdougbrown/whinny/internal/apronws"
)

// historyCache resolves reply-target authors through the client's
// History, caching per message id (a miss is cached as not-found, so a
// reply storm costs one lookup). It satisfies wake.HistoryLookup.
type historyCache struct {
	client *apronws.Client
	mu     sync.Mutex
	byID   map[string]histEntry
}

type histEntry struct {
	author string
	ok     bool
}

func newHistoryCache(client *apronws.Client) *historyCache {
	return &historyCache{client: client, byID: make(map[string]histEntry)}
}

// lookup returns the author of messageID, ok=false when the room's
// fetched window does not contain it.
func (h *historyCache) lookup(ctx context.Context, messageID string) (string, bool, error) {
	h.mu.Lock()
	if e, ok := h.byID[messageID]; ok {
		h.mu.Unlock()
		return e.author, e.ok, nil
	}
	h.mu.Unlock()
	res, err := h.client.History(ctx, apronws.HistoryParams{Limit: HistoryLookupLimit})
	if err != nil {
		// An error is not cached: the next reply gets a fresh chance.
		return "", false, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range res.Messages {
		if _, seen := h.byID[m.MessageID]; !seen {
			h.byID[m.MessageID] = histEntry{author: m.From.UserID, ok: true}
		}
	}
	// A target outside the fetched window is a miss, cached as such.
	if e, ok := h.byID[messageID]; ok {
		return e.author, e.ok, nil
	}
	h.byID[messageID] = histEntry{}
	return "", false, nil
}
