package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/daemon"
	"github.com/sdougbrown/whinny/internal/store"
	"github.com/sdougbrown/whinny/internal/watch"
)

// MaxTTL is the hard cap on a watch's time to live (7d).
const MaxTTL = 168 * time.Hour

// DefaultTTL is the watch TTL when --ttl is absent (24h).
const DefaultTTL = 24 * time.Hour

// AddWatchHandler runs `whinny watch add`: it validates the condition
// flags, resolves the room, and stores the watch.
func AddWatchHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		from := inv.Flags["from"]
		pattern := inv.Flags["pattern"]
		thread := inv.Flags["thread"]
		replyTo := inv.Flags["reply-to"]
		if from == "" && pattern == "" && thread == "" && replyTo == "" {
			return nil, UsageErrorf("watch add: at least one of --from, --pattern, --thread, --reply-to is required")
		}
		if inv.Bools["once"] && inv.Bools["persistent"] {
			return nil, UsageErrorf("watch add: --once and --persistent are mutually exclusive")
		}
		if pattern != "" && thread != "" {
			return nil, UsageErrorf("watch add: --pattern and --thread are mutually exclusive")
		}
		if pattern != "" && replyTo != "" {
			return nil, UsageErrorf("watch add: --pattern and --reply-to are mutually exclusive")
		}
		owner := inv.Flags["owner"]
		onFire := inv.Flags["on-fire"]
		if owner == "" && onFire != "" {
			return nil, UsageErrorf("watch add: --on-fire requires --owner")
		}
		if owner != "" && onFire == "" {
			return nil, UsageErrorf("watch add: --owner requires --on-fire")
		}
		ttl := inv.Durations["ttl"]
		if ttl == 0 {
			ttl = DefaultTTL
		}
		if ttl <= 0 {
			return nil, UsageErrorf("watch add: --ttl must be positive")
		}
		if ttl > MaxTTL {
			return nil, UsageErrorf("watch add: --ttl %s exceeds the %s cap", ttl, MaxTTL)
		}
		if pattern != "" {
			if _, err := regexp.Compile(pattern); err != nil {
				return nil, UsageErrorf("watch add: --pattern does not compile: %v", err)
			}
		}
		client, owned, err := d.connect(ctx, inv)
		if err != nil {
			return nil, err
		}
		if owned {
			defer client.Close()
		}
		rooms, err := client.RoomList(ctx, apronws.RoomListParams{})
		if err != nil {
			return nil, OperationalErrorf("room list: %v", err)
		}
		room, err := ResolveRoom(rooms.Joined, rooms.NotJoined, inv.Flags["room"])
		if err != nil {
			return nil, err
		}
		st, err := d.store()
		if err != nil {
			return nil, err
		}
		if d.Store == nil {
			defer st.Close()
		}
		w := buildWatch(room.ID, from, pattern, thread, replyTo, inv.Flags["note"], inv.Bools["persistent"])
		if owner != "" {
			w.Owner = owner
		}
		stored, err := st.AddWatch(ctx, w, int(ttl/time.Second), d.now())
		if err != nil {
			return nil, OperationalErrorf("add watch: %v", err)
		}
		if owner != "" {
			if err := st.SetMeta(ctx, daemon.MetaDeliveryPrefix+owner, onFire); err != nil {
				return nil, OperationalErrorf("set delivery callback: %v", err)
			}
		}
		return stored.ID, nil
	}
}

// buildWatch maps the condition flags to a store watch. The primary
// condition flag sets the kind (reply-to, then pattern, then thread);
// --from is a sender filter on any kind. A --from-only watch is stored
// as a match-all pattern: the sender filter does the work.
func buildWatch(roomID, from, pattern, thread, replyTo, note string, persistent bool) watch.Watch {
	kind := watch.KindPattern
	switch {
	case replyTo != "":
		kind = watch.KindEdge
	case pattern == "" && thread != "":
		kind = watch.KindThread
	}
	if pattern == "" && replyTo == "" && thread == "" {
		pattern = ".*"
	}
	once := true
	if persistent {
		once = false
	}
	return watch.Watch{
		RoomID:   roomID,
		Kind:     kind,
		FromUser: from,
		Pattern:  pattern,
		ThreadID: thread,
		ReplyTo:  replyTo,
		Note:     note,
		Once:     &once,
	}
}

// CancelWatchHandler runs `whinny watch cancel`: it removes the watch
// by id and reports whether one was removed.
func CancelWatchHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		if len(inv.Args) == 0 {
			return nil, UsageErrorf("watch cancel: missing watch id")
		}
		id := inv.Args[0]
		st, err := d.store()
		if err != nil {
			return nil, err
		}
		if d.Store == nil {
			defer st.Close()
		}
		removed := false
		if _, err := st.GetWatch(ctx, id); err == nil {
			if err := st.DeleteWatch(ctx, id); err != nil {
				return nil, OperationalErrorf("delete watch: %v", err)
			}
			removed = true
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, OperationalErrorf("get watch: %v", err)
		}
		if removed {
			return "true", nil
		}
		return "false", nil
	}
}

// watchJSON is one watch's JSON projection.
type watchJSON struct {
	ID        string `json:"id"`
	RoomID    string `json:"room_id"`
	Kind      string `json:"kind"`
	FromUser  string `json:"from_user,omitempty"`
	Pattern   string `json:"pattern,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
	ReplyTo   string `json:"reply_to,omitempty"`
	Note      string `json:"note,omitempty"`
	Once      bool   `json:"once"`
	ExpiresAt int64  `json:"expires_at"`
}

// ListWatchesHandler runs `whinny watch list`: it prints the stored
// watches as `id  room  kind  note  expires` lines, or JSON with
// --json.
func ListWatchesHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		st, err := d.store()
		if err != nil {
			return nil, err
		}
		if d.Store == nil {
			defer st.Close()
		}
		ws, err := st.ListWatches(ctx)
		if err != nil {
			return nil, OperationalErrorf("list watches: %v", err)
		}
		if inv.Bools["json"] {
			out := make([]watchJSON, 0, len(ws))
			for _, w := range ws {
				once := w.Once == nil || *w.Once
				out = append(out, watchJSON{
					ID:        w.ID,
					RoomID:    w.RoomID,
					Kind:      string(w.Kind),
					FromUser:  w.FromUser,
					Pattern:   w.Pattern,
					ThreadID:  w.ThreadID,
					ReplyTo:   w.ReplyTo,
					Note:      w.Note,
					Once:      once,
					ExpiresAt: w.ExpiresAt,
				})
			}
			return out, nil
		}
		var b strings.Builder
		for _, w := range ws {
			fmt.Fprintf(&b, "%s  %s  %s  %s  %s\n",
				w.ID, w.RoomID, w.Kind, w.Note,
				time.Unix(w.ExpiresAt, 0).UTC().Format(time.RFC3339))
		}
		return strings.TrimSuffix(b.String(), "\n"), nil
	}
}
