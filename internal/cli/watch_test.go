package cli

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/daemon"
	"github.com/sdougbrown/whinny/internal/store"
	"github.com/sdougbrown/whinny/internal/watch"
)

// watchDeps builds Deps with a fake client and a temp-dir store.
func watchDeps(t *testing.T) (Deps, *store.Store) {
	t.Helper()
	c := &fakeClient{joined: []apronws.Room{{ID: "r1", Title: "General"}}}
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	d := testDeps(c)
	d.Store = s
	return d, s
}

func TestWatchAddValidation(t *testing.T) {
	d, _ := watchDeps(t)
	ctx := context.Background()
	cases := []struct {
		name  string
		flags map[string]string
		bools map[string]bool
		want  string
	}{
		{"no condition", map[string]string{"room": "r1"}, nil, "at least one of"},
		{"once and persistent", map[string]string{"room": "r1", "pattern": "x"}, map[string]bool{"once": true, "persistent": true}, "mutually exclusive"},
		{"pattern and thread", map[string]string{"room": "r1", "pattern": "x", "thread": "m1"}, nil, "mutually exclusive"},
		{"pattern and reply-to", map[string]string{"room": "r1", "pattern": "x", "reply-to": "m1"}, nil, "mutually exclusive"},
		{"thread and reply-to", map[string]string{"room": "r1", "thread": "m1", "reply-to": "m1"}, nil, "mutually exclusive"},
		{"ttl over cap", map[string]string{"room": "r1", "pattern": "x"}, map[string]bool{}, "exceeds the"},
		{"bad pattern", map[string]string{"room": "r1", "pattern": "("}, nil, "does not compile"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i := inv("watch add", nil, tc.flags, tc.bools)
			if tc.name == "ttl over cap" {
				// The ttl flag is a duration; feed it through Durations.
				i.Durations = map[string]time.Duration{"ttl": 200 * time.Hour}
			}
			_, err := AddWatchHandler(d)(ctx, i, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestWatchAddDefaultsAndRoundTrip(t *testing.T) {
	d, s := watchDeps(t)
	ctx := context.Background()
	res, err := AddWatchHandler(d)(ctx,
		inv("watch add", nil, map[string]string{"room": "general", "pattern": "deploy", "note": "watch the deploys"}, nil), io.Discard)
	if err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	id, ok := res.(string)
	if !ok || id == "" {
		t.Fatalf("result = %v, want the watch id", res)
	}
	ws, err := s.ListWatches(ctx)
	if err != nil {
		t.Fatalf("ListWatches: %v", err)
	}
	if len(ws) != 1 {
		t.Fatalf("watches = %d, want 1", len(ws))
	}
	w := ws[0]
	if w.ID != id {
		t.Fatalf("stored id = %q, want %q", w.ID, id)
	}
	if w.RoomID != "r1" {
		t.Fatalf("RoomID = %q, want the resolved room", w.RoomID)
	}
	if w.Kind != watch.KindPattern || w.Pattern != "deploy" {
		t.Fatalf("kind/pattern = %q/%q, want pattern/deploy", w.Kind, w.Pattern)
	}
	if w.Note != "watch the deploys" {
		t.Fatalf("Note = %q, want the note", w.Note)
	}
	if w.Once == nil || !*w.Once {
		t.Fatalf("Once = %v, want the once default", w.Once)
	}
	if got := w.ExpiresAt - w.CreatedAt; got != int64(store.DefaultTTLSeconds) {
		t.Fatalf("ttl = %d, want the 24h default", got)
	}
}

func TestWatchAddPersistent(t *testing.T) {
	d, s := watchDeps(t)
	if _, err := AddWatchHandler(d)(context.Background(),
		inv("watch add", nil, map[string]string{"room": "r1", "pattern": "x"}, map[string]bool{"persistent": true}), io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	ws, _ := s.ListWatches(context.Background())
	if len(ws) != 1 || ws[0].Once == nil || *ws[0].Once {
		t.Fatalf("Once = %v, want an explicit false", ws)
	}
}

func TestWatchAddFromOnly(t *testing.T) {
	d, s := watchDeps(t)
	if _, err := AddWatchHandler(d)(context.Background(),
		inv("watch add", nil, map[string]string{"room": "r1", "from": "u1"}, nil), io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	ws, _ := s.ListWatches(context.Background())
	if len(ws) != 1 {
		t.Fatalf("watches = %d, want 1", len(ws))
	}
	w := ws[0]
	if w.Kind != watch.KindPattern || w.Pattern != ".*" {
		t.Fatalf("kind/pattern = %q/%q, want a match-all pattern", w.Kind, w.Pattern)
	}
	if w.FromUser != "u1" {
		t.Fatalf("FromUser = %q, want u1", w.FromUser)
	}
}

func TestWatchAddReplyTo(t *testing.T) {
	d, s := watchDeps(t)
	if _, err := AddWatchHandler(d)(context.Background(),
		inv("watch add", nil, map[string]string{"room": "r1", "reply-to": "m1"}, nil), io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	ws, _ := s.ListWatches(context.Background())
	if ws[0].Kind != watch.KindEdge || ws[0].ReplyTo != "m1" {
		t.Fatalf("watch = %+v, want edge/m1", ws[0])
	}
}

func TestWatchAddThread(t *testing.T) {
	d, s := watchDeps(t)
	if _, err := AddWatchHandler(d)(context.Background(),
		inv("watch add", nil, map[string]string{"room": "r1", "thread": "m1"}, nil), io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	ws, _ := s.ListWatches(context.Background())
	if ws[0].Kind != watch.KindThread || ws[0].ThreadID != "m1" {
		t.Fatalf("watch = %+v, want thread/m1", ws[0])
	}
}

func TestWatchAddTTLAtCap(t *testing.T) {
	d, s := watchDeps(t)
	i := inv("watch add", nil, map[string]string{"room": "r1", "pattern": "x"}, nil)
	i.Durations = map[string]time.Duration{"ttl": MaxTTL}
	if _, err := AddWatchHandler(d)(context.Background(), i, io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	ws, _ := s.ListWatches(context.Background())
	if got := ws[0].ExpiresAt - ws[0].CreatedAt; got != int64(store.MaxTTLSeconds) {
		t.Fatalf("ttl = %d, want the 168h cap", got)
	}
}

func TestWatchCancel(t *testing.T) {
	d, s := watchDeps(t)
	ctx := context.Background()
	if _, err := AddWatchHandler(d)(ctx,
		inv("watch add", nil, map[string]string{"room": "r1", "pattern": "x"}, nil), io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	ws, _ := s.ListWatches(ctx)
	res, err := CancelWatchHandler(d)(ctx, inv("watch cancel", []string{ws[0].ID}, nil, nil), io.Discard)
	if err != nil {
		t.Fatalf("CancelWatchHandler: %v", err)
	}
	if res != "true" {
		t.Fatalf("result = %v, want true", res)
	}
	if ws, _ := s.ListWatches(ctx); len(ws) != 0 {
		t.Fatalf("watches = %d, want 0 after cancel", len(ws))
	}
	res, err = CancelWatchHandler(d)(ctx, inv("watch cancel", []string{"missing"}, nil, nil), io.Discard)
	if err != nil {
		t.Fatalf("CancelWatchHandler: %v", err)
	}
	if res != "false" {
		t.Fatalf("result = %v, want false for a missing id", res)
	}
}

func TestWatchList(t *testing.T) {
	d, s := watchDeps(t)
	ctx := context.Background()
	if _, err := AddWatchHandler(d)(ctx,
		inv("watch add", nil, map[string]string{"room": "r1", "pattern": "x", "note": "n1"}, nil), io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	res, err := ListWatchesHandler(d)(ctx, inv("watch list", nil, nil, nil), io.Discard)
	if err != nil {
		t.Fatalf("ListWatchesHandler: %v", err)
	}
	line, ok := res.(string)
	if !ok {
		t.Fatalf("result = %T, want the human lines", res)
	}
	ws, _ := s.ListWatches(ctx)
	for _, want := range []string{ws[0].ID, "r1", "pattern", "n1"} {
		if !strings.Contains(line, want) {
			t.Fatalf("line = %q, want to contain %q", line, want)
		}
	}
	res, err = ListWatchesHandler(d)(ctx, inv("watch list", nil, nil, map[string]bool{"json": true}), io.Discard)
	if err != nil {
		t.Fatalf("ListWatchesHandler: %v", err)
	}
	jws, ok := res.([]watchJSON)
	if !ok || len(jws) != 1 {
		t.Fatalf("result = %v, want a 1-watch JSON array", res)
	}
}

// TestWatchAddOwnerOnFire: --owner and --on-fire together set the
// watch's Owner field and store the delivery callback under
// "delivery:<owner>".
func TestWatchAddOwnerOnFire(t *testing.T) {
	d, s := watchDeps(t)
	ctx := context.Background()
	if _, err := AddWatchHandler(d)(ctx,
		inv("watch add", nil, map[string]string{
			"room":    "r1",
			"pattern": "x",
			"owner":   "agent-x",
			"on-fire": "notify --room r1",
		}, nil), io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	ws, err := s.ListWatches(ctx)
	if err != nil {
		t.Fatalf("ListWatches: %v", err)
	}
	if len(ws) != 1 {
		t.Fatalf("watches = %d, want 1", len(ws))
	}
	if ws[0].Owner != "agent-x" {
		t.Fatalf("Owner = %q, want agent-x", ws[0].Owner)
	}
	cmd, err := s.GetMeta(ctx, daemon.MetaDeliveryPrefix+"agent-x")
	if err != nil {
		t.Fatalf("GetMeta delivery: %v", err)
	}
	if cmd != "notify --room r1" {
		t.Fatalf("delivery callback = %q, want the on-fire command", cmd)
	}
}

// TestWatchAddOwnerOnFireValidation: --on-fire without --owner (and
// --owner without --on-fire) are usage errors; the pair is required
// together.
func TestWatchAddOwnerOnFireValidation(t *testing.T) {
	d, _ := watchDeps(t)
	ctx := context.Background()
	cases := []struct {
		name  string
		flags map[string]string
		want  string
	}{
		{"on-fire without owner", map[string]string{"room": "r1", "pattern": "x", "on-fire": "cmd"}, "requires --owner"},
		{"owner without on-fire", map[string]string{"room": "r1", "pattern": "x", "owner": "agent-x"}, "requires --on-fire"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := AddWatchHandler(d)(ctx, inv("watch add", nil, tc.flags, nil), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

// TestWatchAddResidentDefaultOwner: a plain resident-mode watch add
// (no --owner/--on-fire) keeps the Owner default 'resident' and stores
// no delivery callback.
func TestWatchAddResidentDefaultOwner(t *testing.T) {
	d, s := watchDeps(t)
	ctx := context.Background()
	if _, err := AddWatchHandler(d)(ctx,
		inv("watch add", nil, map[string]string{"room": "r1", "pattern": "x"}, nil), io.Discard); err != nil {
		t.Fatalf("AddWatchHandler: %v", err)
	}
	ws, _ := s.ListWatches(ctx)
	if len(ws) != 1 {
		t.Fatalf("watches = %d, want 1", len(ws))
	}
	if ws[0].Owner != "resident" {
		t.Fatalf("Owner = %q, want the resident default", ws[0].Owner)
	}
	if cmd, _ := s.GetMeta(ctx, daemon.MetaDeliveryPrefix+"resident"); cmd != "" {
		t.Fatalf("delivery callback = %q, want none for a resident watch", cmd)
	}
}
