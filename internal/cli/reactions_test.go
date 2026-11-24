package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/store"
)

func reactionsDeps(t *testing.T) (Deps, *fakeClient, *store.Store) {
	t.Helper()
	c := &fakeClient{
		joined: []apronws.Room{{ID: "r1", Title: "General"}},
	}
	c.capabilities = []string{"reactions"}
	st, err := store.Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	d := testDeps(c)
	d.Store = st
	return d, c, st
}

func TestReactionsHandlerSetsEmoji(t *testing.T) {
	d, c, _ := reactionsDeps(t)
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err != nil {
		t.Fatalf("reactions: %v", err)
	}
	if len(c.reactionSets) != 1 {
		t.Fatalf("SetReactions calls = %d, want 1", len(c.reactionSets))
	}
	call := c.reactionSets[0]
	if call.MessageID != "m1" || len(call.Emojis) != 1 || call.Emojis[0] != "👍" {
		t.Errorf("call = %+v, want m1/[👍]", call)
	}
}

func TestReactionsHandlerClears(t *testing.T) {
	d, c, _ := reactionsDeps(t)
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", nil, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err != nil {
		t.Fatalf("reactions clear: %v", err)
	}
	if len(c.reactionSets) != 1 || len(c.reactionSets[0].Emojis) != 0 {
		t.Errorf("calls = %+v, want one empty set (clear)", c.reactionSets)
	}
}

func TestReactionsHandlerMultipleEmoji(t *testing.T) {
	d, c, _ := reactionsDeps(t)
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍", ":party:"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err != nil {
		t.Fatalf("reactions: %v", err)
	}
	if len(c.reactionSets[0].Emojis) != 2 {
		t.Errorf("emojis = %v, want 2", c.reactionSets[0].Emojis)
	}
}

func TestReactionsHandlerServerUnknownMessageIsUsageError(t *testing.T) {
	// The server names an unknown message_id in its invalid_params
	// error (§4.7: the request carries no room_id; the server resolves
	// the message and checks the caller's visibility). That is a
	// caller mistake: it must surface as a usage error that refunds
	// the rate token, never count toward the breaker.
	d, c, _ := reactionsDeps(t)
	c.setReactionsErr = &apronws.Error{Code: -32602, Message: `Unknown message "m9"`}
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m9"}, nil), nil)
	if err == nil || !strings.Contains(err.Error(), `Unknown message "m9"`) {
		t.Fatalf("error = %v, want the server's message carried through", err)
	}
	v, err := d.Store.GetMeta(context.Background(), store.MetaPostFailures)
	if err != nil {
		t.Fatalf("get failures: %v", err)
	}
	if v != "" && v != "0" {
		t.Errorf("consecutive failures = %q, want 0 (invalid params is a usage error)", v)
	}
}

func TestReactionsHandlerMessageInNotJoinedRoomReacts(t *testing.T) {
	// Reactions only need a room the caller can see: resolution reaches
	// not-joined rooms by id, and the server checks visibility itself.
	d, c, _ := reactionsDeps(t)
	c.joined = nil
	c.notJoined = []apronws.Room{{ID: "r1", Title: "General"}}
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err != nil {
		t.Fatalf("reactions: %v", err)
	}
	if len(c.reactionSets) != 1 {
		t.Errorf("SetReactions calls = %d, want 1", len(c.reactionSets))
	}
	if len(c.histories) != 0 {
		t.Errorf("History called %d times, want 0 (no probe round trip)", len(c.histories))
	}
}

func TestReactionsHandlerRefusesQuiet(t *testing.T) {
	d, c, st := reactionsDeps(t)
	if err := st.SetMeta(context.Background(), MetaQuietUntil, "9999999999"); err != nil {
		t.Fatalf("set quiet: %v", err)
	}
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "refused-quiet") {
		t.Fatalf("error = %v, want refused-quiet", err)
	}
	if len(c.reactionSets) != 0 {
		t.Errorf("SetReactions called, want 0")
	}
}

func TestReactionsHandlerRefusesBreaker(t *testing.T) {
	d, c, st := reactionsDeps(t)
	if err := st.SetBreaker(context.Background(), store.Breaker{Latched: true, Reason: "test"}); err != nil {
		t.Fatalf("set breaker: %v", err)
	}
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "refused-breaker") {
		t.Fatalf("error = %v, want refused-breaker", err)
	}
	if len(c.reactionSets) != 0 {
		t.Errorf("SetReactions called, want 0")
	}
}

func TestReactionsHandlerRateLimitCountsTowardBreaker(t *testing.T) {
	d, c, _ := reactionsDeps(t)
	d.Env = func(k string) string {
		if k == "WHINNY_RATE_PER_MINUTE" {
			return "1"
		}
		return ""
	}
	// The first set consumes the bucket's only token; the second is refused.
	if _, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil); err != nil {
		t.Fatalf("first reactions: %v", err)
	}
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"🎉"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "refused-rate-limit") {
		t.Fatalf("error = %v, want refused-rate-limit", err)
	}
	if len(c.reactionSets) != 1 {
		t.Errorf("SetReactions calls = %d, want 1 (only the first, allowed send)", len(c.reactionSets))
	}
	// The refused send is a bad outcome: the breaker must have tripped
	// (five consecutive failures is the default; a zero-rate bucket
	// refuses every attempt, so the first refusal already counts one).
	v, err := d.Store.GetMeta(context.Background(), store.MetaPostFailures)
	if err != nil {
		t.Fatalf("get failures: %v", err)
	}
	if v != "1" {
		t.Errorf("consecutive failures = %q, want 1", v)
	}
}

func TestReactionsHandlerUsageErrors(t *testing.T) {
	d, c, _ := reactionsDeps(t)
	// No --message.
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1"}, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "--message") {
		t.Errorf("error = %v, want missing --message", err)
	}
	// No --room with --list? --list needs no room; but a set without
	// --room is a room resolution failure (no room matches "").
	_, err = d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"message": "m1"}, nil), nil)
	if err == nil {
		t.Error("reactions without --room succeeded, want room error")
	}
	if len(c.reactionSets) != 0 {
		t.Errorf("SetReactions called, want 0")
	}
}

func TestReactionsHandlerListWithoutRoom(t *testing.T) {
	d, c, _ := reactionsDeps(t)
	c.emojiCatalog = map[string]apronws.EmojiEntry{":party:": {URL: "http://x/p.png"}}
	out, err := d.ReactionsHandler(context.Background(), inv("reactions", nil, nil, map[string]bool{"list": true}), nil)
	if err != nil {
		t.Fatalf("reactions --list: %v", err)
	}
	text, ok := out.(string)
	if !ok {
		t.Fatalf("output = %T, want string", out)
	}
	if !strings.Contains(text, "reactions: supported") {
		t.Errorf("list text = %q, want the supported line (fake advertises reactions)", text)
	}
	if !strings.Contains(text, ":party:") {
		t.Errorf("list text = %q, want the custom emoji", text)
	}
}

func TestReactionsHandlerSlackRefusal(t *testing.T) {
	// The slack transport's SetReactions refusal is a usage error; the
	// handler must refund the rate token for it (no breaker count).
	d, _, _ := reactionsDeps(t)
	sc := &refusingClient{}
	d.Client = sc
	d.Env = func(string) string { return "" }
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err == nil {
		t.Fatal("reactions on refusing client succeeded")
	}
	v, err := d.Store.GetMeta(context.Background(), store.MetaPostFailures)
	if err != nil {
		t.Fatalf("get failures: %v", err)
	}
	if v != "" && v != "0" {
		t.Errorf("consecutive failures = %q, want 0 (usage error refunds)", v)
	}
}

// refusingClient refuses SetReactions with a usage error, the way the
// slack transport does.
type refusingClient struct{ fakeClient }

func (f *refusingClient) SetReactions(_ context.Context, _ string, _ []string) error {
	return UsageErrorf("reactions are not supported on the Slack transport yet")
}

func TestReactionsHandlerListJSON(t *testing.T) {
	d, c, _ := reactionsDeps(t)
	c.emojiCatalog = map[string]apronws.EmojiEntry{":party:": {URL: "http://x/p.png"}}
	out, err := d.ReactionsHandler(context.Background(), inv("reactions", nil, nil, map[string]bool{"list": true, "json": true}), nil)
	if err != nil {
		t.Fatalf("reactions --list --json: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("output = %T, want map", out)
	}
	if reactions, ok := m["reactions"].(bool); !ok || !reactions {
		t.Errorf("reactions = %v, want true (fake advertises reactions)", m["reactions"])
	}
	emoji, ok := m["emoji"].(map[string]apronws.EmojiEntry)
	if !ok || len(emoji) != 1 {
		t.Errorf("emoji = %v, want the one catalog entry", m["emoji"])
	}
}

func TestReactionsHandlerJSONOutput(t *testing.T) {
	d, _, _ := reactionsDeps(t)
	out, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, map[string]bool{"json": true}), nil)
	if err != nil {
		t.Fatalf("reactions --json: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("output = %T, want map", out)
	}
	if m["message_id"] != "m1" || m["room_id"] != "r1" {
		t.Errorf("output = %v, want message_id m1 room_id r1", m)
	}
	if cleared, _ := m["cleared"].(bool); cleared {
		t.Errorf("cleared = %v, want false", m["cleared"])
	}
}

func TestReactionsHandlerRefusesWithoutCapability(t *testing.T) {
	// A server without the reactions capability refuses as a usage
	// error before any send, so the shared breaker never counts it.
	d, c, _ := reactionsDeps(t)
	c.capabilities = nil
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "does not offer the reactions capability") {
		t.Fatalf("error = %v, want the capability refusal", err)
	}
	if len(c.reactionSets) != 0 {
		t.Errorf("SetReactions called, want 0")
	}
	v, err := d.Store.GetMeta(context.Background(), store.MetaPostFailures)
	if err != nil {
		t.Fatalf("get failures: %v", err)
	}
	if v != "" && v != "0" {
		t.Errorf("consecutive failures = %q, want 0 (capability refusal is a usage error)", v)
	}
}

func TestReactionsHandlerSetReactionsErrorCountsTowardBreaker(t *testing.T) {
	// An operational SetReactions failure is a bad send outcome: the
	// token is refunded and the failure counter bumps.
	d, c, _ := reactionsDeps(t)
	c.setReactionsErr = OperationalErrorf("connection lost")
	_, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil)
	if err == nil || strings.Contains(err.Error(), "usage") {
		t.Fatalf("error = %v, want an operational error", err)
	}
	v, err := d.Store.GetMeta(context.Background(), store.MetaPostFailures)
	if err != nil {
		t.Fatalf("get failures: %v", err)
	}
	if v != "1" {
		t.Errorf("consecutive failures = %q, want 1 (operational send failure counts)", v)
	}
}

func TestReactionsHandlerSuccessResetsFailures(t *testing.T) {
	// A confirmed send resets the consecutive failure counter.
	d, _, st := reactionsDeps(t)
	if err := st.SetMeta(context.Background(), store.MetaPostFailures, "3"); err != nil {
		t.Fatalf("seed failures: %v", err)
	}
	if _, err := d.ReactionsHandler(context.Background(), inv("reactions", []string{"👍"}, map[string]string{"room": "r1", "message": "m1"}, nil), nil); err != nil {
		t.Fatalf("reactions: %v", err)
	}
	v, err := st.GetMeta(context.Background(), store.MetaPostFailures)
	if err != nil {
		t.Fatalf("get failures: %v", err)
	}
	if v != "0" {
		t.Errorf("consecutive failures after success = %q, want 0", v)
	}
}
