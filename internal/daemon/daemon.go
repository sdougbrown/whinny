package daemon

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/store"
	"github.com/sdougbrown/whinny/internal/wake"
	"github.com/sdougbrown/whinny/internal/watch"
)

// Ticker is the expiry-sweep tick source; tests inject one they control.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// realTicker adapts time.Ticker to Ticker.
type realTicker struct{ t *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.t.C }
func (t realTicker) Stop()               { t.t.Stop() }

// newRealTicker builds the production ticker.
func newRealTicker(d time.Duration) Ticker { return realTicker{t: time.NewTicker(d)} }

// Deps carries the injected dependencies. Client may be nil, in which
// case Run connects through apronws itself and owns the client; tests
// inject a client connected to a fake server instead.
type Deps struct {
	Client  *apronws.Client
	Store   *store.Store
	Deliver Deliverer
	// Clock is the policy's time source (ambient gate valves). Nil falls
	// back to Now.
	Clock wake.Clock
	// Now is the wall clock for store timestamps, expiry, and the sweep.
	// Nil falls back to time.Now.
	Now func() time.Time
	// Ticker builds the expiry-sweep tick source. Nil falls back to a
	// real ticker on ExpirySweepInterval.
	Ticker func(time.Duration) Ticker
}

// daemon is the run-loop state: one connected client, one store, one
// deliverer. This struct is the RoomLink seam — when the standalone
// link is extracted from the daemon, it grows from here.
type daemon struct {
	cfg    Config
	store  *store.Store
	client *apronws.Client
	dlv    Deliverer
	now    func() time.Time
	clock  wake.Clock
	gate   *wake.AmbientGate
	class  wake.Classifier
	hist   *historyCache
}

// Run drives the daemon until ctx is done: it connects (when no client
// is injected), drains stale queued wakes for its agent, then loops over
// inbound records and the expiry sweep. It returns nil on clean shutdown
// (ctx cancel or client close).
func Run(ctx context.Context, cfg Config, deps Deps) error {
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	if deps.Store == nil {
		return errNilDep("store")
	}
	if deps.Deliver == nil {
		return errNilDep("deliverer")
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	clock := deps.Clock
	if clock == nil {
		clock = clockFunc(now)
	}
	newTicker := deps.Ticker
	if newTicker == nil {
		newTicker = newRealTicker
	}

	client := deps.Client
	if client == nil {
		var err error
		client, err = apronws.Connect(ctx, apronws.Options{URL: cfg.URL, Token: cfg.Token})
		if err != nil {
			return err
		}
		defer client.Close()
		// The connection is authenticated at this point, so pushes for the
		// bot's rooms arrive from here on. Operators and scripts wait on
		// this line to know the daemon is live.
		log.Printf("daemon: connected to %s", cfg.URL)
	}

	d := &daemon{
		cfg:    cfg,
		store:  deps.Store,
		client: client,
		dlv:    deps.Deliver,
		now:    now,
		clock:  clock,
		gate:   newAmbientGate(cfg),
		class:  classifierClient(cfg),
		hist:   newHistoryCache(client),
	}

	// Reconnect catch-up: when the connection drops and reconnects, the
	// messages missed in the gap are replayed from history (the cursor
	// bounds the window). The hook runs in its own goroutine, off the
	// client's read loop.
	client.OnReconnect = func() {
		d.catchUp(ctx)
	}

	// Drain stale queue entries for this agent before the loop: a wake
	// enqueued before a crash fires now. The store's dequeue is
	// at-most-once; that contract is accepted here.
	if err := d.drain(ctx); err != nil {
		// A canceled context means we are shutting down anyway; the
		// queued wakes stay put for the next start.
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	// Start catch-up: replay the messages missed while the daemon was
	// down, before the live loop begins.
	d.catchUp(ctx)

	subscribe := d.autoSubscribe(ctx)
	tick := newTicker(ExpirySweepInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C():
			d.expireDue(ctx)
		case rec, ok := <-client.Records():
			if !ok {
				return nil
			}
			d.handleRecord(ctx, rec, subscribe)
		}
	}
}

func errNilDep(name string) error {
	return &nilDepError{name: name}
}

type nilDepError struct{ name string }

func (e *nilDepError) Error() string { return "daemon: nil " + e.name + " in Deps" }

// newAmbientGate builds the gate for the ambient config, or nil when the
// gate is disabled.
func newAmbientGate(cfg Config) *wake.AmbientGate {
	if cfg.Ambient.Endpoint == "" {
		return nil
	}
	return wake.NewAmbientGate(cfg.Ambient.MinSpacing, cfg.Ambient.MaxPerHour)
}

// autoSubscribe reads the self-subscription flag once at startup: on
// unless the store meta carries auto_subscribe=off.
func (d *daemon) autoSubscribe(ctx context.Context) bool {
	v, err := d.store.GetMeta(ctx, MetaAutoSubscribe)
	if err != nil {
		log.Printf("daemon: read %s meta: %v", MetaAutoSubscribe, err)
		return true
	}
	return v != MetaAutoSubscribeOff
}

// drain delivers and removes every queued wake for this agent.
func (d *daemon) drain(ctx context.Context) error {
	wakes, err := d.store.DequeueWakes(ctx, d.cfg.Agent)
	if err != nil {
		return err
	}
	for _, w := range wakes {
		if err := d.dlv.Deliver(ctx, w.Agent, decodePayload(w.Payload)); err != nil {
			log.Printf("daemon: deliver queued wake %d: %v", w.ID, err)
		}
	}
	return nil
}

// catchUp replays the messages missed while the daemon was down (or
// disconnected): for each joined room, it fetches the history since the
// room's cursor, runs each message through the record handler
// oldest-first, and lets the handler advance the cursor. Bounded to
// catchUpLimit messages per room.
const catchUpLimit = 200

func (d *daemon) catchUp(ctx context.Context) {
	rooms, err := d.client.RoomList(ctx, apronws.RoomListParams{})
	if err != nil {
		log.Printf("daemon: catch-up room list: %v", err)
		return
	}
	subscribe := d.autoSubscribe(ctx)
	for _, room := range rooms.Joined {
		cursor, err := d.store.GetCursor(ctx, room.ID)
		if err != nil {
			log.Printf("daemon: catch-up cursor %s: %v", room.ID, err)
			continue
		}
		res, err := d.client.History(ctx, apronws.HistoryParams{RoomID: room.ID, Limit: catchUpLimit})
		if err != nil {
			log.Printf("daemon: catch-up history %s: %v", room.ID, err)
			continue
		}
		var msgs []*apronws.Message
		for i := range res.Messages {
			if int64(res.Messages[i].LogID) > cursor {
				msgs = append(msgs, &res.Messages[i])
			}
		}
		sort.Slice(msgs, func(i, j int) bool { return int64(msgs[i].LogID) < int64(msgs[j].LogID) })
		for _, m := range msgs {
			d.handleRecord(ctx, apronws.Record{Kind: apronws.KindMessage, Message: m}, subscribe)
		}
	}
}

// handleRecord processes one inbound push. Only message records carry
// policy weight; room updates and activities are ignored.
func (d *daemon) handleRecord(ctx context.Context, rec apronws.Record, subscribe bool) {
	if rec.Kind != apronws.KindMessage || rec.Message == nil {
		return
	}
	m := rec.Message
	// Feed the reply-author cache as records stream past, so a later
	// reply to this message resolves without a History call. The author
	// is known even for a delete tombstone.
	d.hist.feed(m.MessageID, m.RoomID, m.From.UserID)
	if err := d.store.SetCursor(ctx, m.RoomID, int64(m.LogID)); err != nil {
		log.Printf("daemon: set cursor %s: %v", m.RoomID, err)
	}
	// A delete tombstone carries no body: nothing to evaluate, and the
	// auto-subscription anchors on a body-carrying post.
	if m.Body == nil || m.Deleted {
		return
	}
	// The bot's own posts arm thread subscriptions and are never
	// evaluated against the policy: no self-wake loops.
	if m.From.UserID == d.cfg.Self.UserID {
		if subscribe && m.ReplyTo != nil {
			d.subscribeOwnThread(ctx, m)
		}
		return
	}
	if d.cfg.Mode == ModeShared {
		d.handleShared(ctx, m)
		return
	}
	d.handleResident(ctx, m)
}

// subscribeOwnThread arms a subscription watch on the thread the bot
// just posted to: thread posts anchor at the parent message, so the
// push's reply_to is the thread root. Owner is this daemon's agent; TTL
// is one day.
func (d *daemon) subscribeOwnThread(ctx context.Context, m *apronws.Message) {
	_, err := d.store.AddWatch(ctx, store.Watch{
		RoomID:   m.RoomID,
		Owner:    d.cfg.Agent,
		Kind:     watch.KindSubscription,
		ThreadID: m.ReplyTo.MessageID,
		Note:     "my thread post",
	}, DefaultSubscribeTTL, d.now())
	if err != nil {
		log.Printf("daemon: subscribe own thread: %v", err)
	}
}

// handleResident runs the full policy for one message and acts on the
// decision: enqueue for durability, then deliver immediately, then
// remove fired once-watches.
func (d *daemon) handleResident(ctx context.Context, m *apronws.Message) {
	msg := mapMsg(m)
	watches := d.roomWatches(ctx, m.RoomID)
	var resident []store.Watch
	for _, w := range watches {
		if d.ownsWatch(w.Owner) {
			resident = append(resident, w)
		}
	}
	dec := wake.Evaluate(ctx, wake.In{
		Msg:        msg,
		Self:       d.cfg.Self,
		Peers:      d.cfg.Peers,
		Watches:    resident,
		History:    func(ctx context.Context, messageID string) (string, bool, error) { return d.hist.lookup(ctx, m.RoomID, messageID) },
		Classifier: d.class,
		Gate:       d.gate,
		Clock:      d.clock,
	})
	if !dec.Wake {
		return
	}
	env := BuildEnvelope(dec, msg)
	env.ImageURL = firstImageURL(m)
	payload, err := encodePayload(env)
	if err != nil {
		log.Printf("daemon: encode wake: %v", err)
		return
	}
	if _, err := d.store.EnqueueWake(ctx, store.Wake{Agent: d.cfg.Agent, Payload: payload}, d.now()); err != nil {
		log.Printf("daemon: enqueue wake: %v", err)
		return
	}
	d.flushQueue(ctx)
	for _, id := range dec.RemoveWatchIDs {
		if err := d.store.DeleteWatch(ctx, id); err != nil {
			log.Printf("daemon: delete watch %s: %v", id, err)
		}
	}
}

// flushQueue delivers and removes every queued wake for this agent.
func (d *daemon) flushQueue(ctx context.Context) {
	wakes, err := d.store.DequeueWakes(ctx, d.cfg.Agent)
	if err != nil {
		log.Printf("daemon: dequeue wakes: %v", err)
		return
	}
	for _, w := range wakes {
		if err := d.dlv.Deliver(ctx, w.Agent, decodePayload(w.Payload)); err != nil {
			log.Printf("daemon: deliver wake %d: %v", w.ID, err)
		}
	}
}

// handleShared routes watch hits to their owners. No mention/reply/ambient
// evaluation, no durable queue: a delivery failure is logged and dropped,
// because the transient run the wake belonged to may be gone.
func (d *daemon) handleShared(ctx context.Context, m *apronws.Message) {
	msg := mapMsg(m)
	dec := wake.Evaluate(ctx, wake.In{
		Msg:     msg,
		Self:    d.cfg.Self,
		Peers:   d.cfg.Peers,
		Watches: d.roomWatches(ctx, m.RoomID),
		Clock:   d.clock,
	})
	// Mention/reply/ambient decisions carry no watch hits here; only
	// watch fires are acted on.
	for _, hit := range dec.Watches {
		hitDec := wake.Decision{
			Wake:    true,
			Kind:    wake.KindWatch,
			Reason:  hit.Note,
			Watches: []wake.WatchHit{hit},
		}
		env := BuildEnvelope(hitDec, msg)
		env.ImageURL = firstImageURL(m)
		if cmd := d.ownerCommand(ctx, hit.Owner); len(cmd) > 0 {
			(&execDeliverer{cmd: cmd}).Deliver(ctx, hit.Owner, env)
			continue
		}
		if err := d.dlv.Deliver(ctx, hit.Owner, env); err != nil {
			log.Printf("daemon: deliver watch wake to %s: %v", hit.Owner, err)
		}
	}
	for _, id := range dec.RemoveWatchIDs {
		if err := d.store.DeleteWatch(ctx, id); err != nil {
			log.Printf("daemon: delete watch %s: %v", id, err)
		}
	}
}

// ownerCommand returns the shell-split delivery callback stored under
// "delivery:<owner>", or nil when the owner has none.
func (d *daemon) ownerCommand(ctx context.Context, owner string) []string {
	v, err := d.store.GetMeta(ctx, MetaDeliveryPrefix+owner)
	if err != nil {
		log.Printf("daemon: read delivery callback for %s: %v", owner, err)
		return nil
	}
	return SplitCommand(v)
}

// ownsWatch reports whether this resident daemon should act on a watch
// with owner: 'resident' (the default) and the daemon's own agent are
// kept; anything else belongs to a shared-mode transient with its own
// callback and must not be consumed here.
func (d *daemon) ownsWatch(owner string) bool {
	return owner == "resident" || owner == d.cfg.Agent
}

// expireDue delivers expiry envelopes for every due watch and deletes
// the watch: expiry is a wake the agent must see to re-arm. In resident
// mode the envelope routes through the durable queue under cfg.Agent
// (matching the message path); foreign-owner watches are skipped.
func (d *daemon) expireDue(ctx context.Context) {
	due, err := d.store.DueWatches(ctx, d.now())
	if err != nil {
		log.Printf("daemon: list due watches: %v", err)
		return
	}
	for _, w := range due {
		if d.cfg.Mode == ModeResident && !d.ownsWatch(w.Owner) {
			continue
		}
		env := ExpiryEnvelope(w)
		if d.cfg.Mode == ModeShared {
			if cmd := d.ownerCommand(ctx, w.Owner); len(cmd) > 0 {
				(&execDeliverer{cmd: cmd}).Deliver(ctx, w.Owner, env)
			} else if err := d.dlv.Deliver(ctx, w.Owner, env); err != nil {
				log.Printf("daemon: deliver expiry to %s: %v", w.Owner, err)
			}
		} else {
			payload, err := encodePayload(env)
			if err != nil {
				log.Printf("daemon: encode expiry: %v", err)
				continue
			}
			if _, err := d.store.EnqueueWake(ctx, store.Wake{Agent: d.cfg.Agent, Payload: payload}, d.now()); err != nil {
				log.Printf("daemon: enqueue expiry: %v", err)
				continue
			}
			d.flushQueue(ctx)
		}
		if err := d.store.DeleteWatch(ctx, w.ID); err != nil {
			log.Printf("daemon: delete expired watch %s: %v", w.ID, err)
		}
	}
}

// roomWatches lists the store's unexpired watches for one room.
func (d *daemon) roomWatches(ctx context.Context, roomID string) []store.Watch {
	ws, err := d.store.WatchesForRoom(ctx, roomID, d.now())
	if err != nil {
		log.Printf("daemon: list watches for %s: %v", roomID, err)
		return nil
	}
	return ws
}

// mapMsg projects an apronws message onto the policy's Msg: mentions and
// embeds decode leniently; pattern matching covers embed titles and
// captions because attachments ride as embeds.
func mapMsg(m *apronws.Message) wake.Msg {
	msg := wake.Msg{
		MessageID: m.MessageID,
		RoomID:    m.RoomID,
		From:      wake.From{UserID: m.From.UserID, Name: m.From.Name},
	}
	if m.ReplyTo != nil {
		msg.ReplyTo = m.ReplyTo.MessageID
	}
	if m.Body != nil {
		msg.Text = m.Body.Text
		msg.Mentions = apronws.DecodeMentions(m.Body.Mentions)
		for _, e := range apronws.DecodeEmbeds(m.Body.Embeds) {
			msg.Embeds = append(msg.Embeds, wake.Embed{Title: e.Title, Caption: e.Caption})
		}
	}
	return msg
}

// firstImageURL returns the first image embed's URL, "" when the message
// carries none.
func firstImageURL(m *apronws.Message) string {
	if m.Body == nil {
		return ""
	}
	for _, e := range apronws.DecodeEmbeds(m.Body.Embeds) {
		if e.Kind == "image" && e.URL != "" {
			return e.URL
		}
	}
	return ""
}

// encodePayload marshals an envelope for the wake queue so a drained
// wake keeps its from/image routing metadata.
func encodePayload(env Envelope) (string, error) {
	b, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodePayload rebuilds an envelope from a queued payload; a legacy
// plain-text payload (or anything unparseable) degrades to a text-only
// envelope.
func decodePayload(payload string) Envelope {
	var env Envelope
	if err := json.Unmarshal([]byte(payload), &env); err == nil && env.Text != "" {
		return env
	}
	return Envelope{Text: payload}
}

// SplitCommand shell-splits a command string into words, honoring
// single- and double-quoted spans. An unterminated quote keeps the rest
// of the string as one word.
func SplitCommand(s string) []string {
	var out []string
	var cur strings.Builder
	inWord := false
	var quote rune
	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
			inWord = true
		case r == ' ' || r == '\t':
			if inWord {
				out = append(out, cur.String())
				cur.Reset()
				inWord = false
			}
		default:
			cur.WriteRune(r)
			inWord = true
		}
	}
	if inWord {
		out = append(out, cur.String())
	}
	return out
}
