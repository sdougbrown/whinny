package daemon

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"sync"
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
	// Wait is the pause between history-lookup retries. Nil falls back
	// to time.Sleep.
	Wait func(time.Duration)
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
	wait   func(time.Duration)
	// procMu serializes record handling between the live loop and the
	// reconnect catch-up goroutine: the cursor dedupe is check-then-act
	// (GetCursor then SetCursor, with no store-side lock spanning both),
	// so an unlocked overlap lets one message evaluate twice. Held only
	// across the dedupe check, evaluation, and enqueue; delivery (the
	// queue flush and shared callbacks) runs after the lock is released
	// so a slow deliverer cannot stall live processing or catch-up.
	// Deliver must not re-enter handleRecord.
	procMu sync.Mutex
}

// Run drives the daemon until ctx is done: it validates the config
// (including the delivery backend, before any connection is attempted),
// connects (when no client is injected), drains stale queued wakes for
// its agent, then loops over inbound records and the expiry sweep. It
// returns nil on clean shutdown (ctx cancel or client close).
func Run(ctx context.Context, cfg Config, deps Deps) error {
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	// The backend is validated before the connection is attempted: a
	// misconfigured backend is rejected without touching the network.
	if err := cfg.ValidateBackend(); err != nil {
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
	wait := deps.Wait
	if wait == nil {
		wait = time.Sleep
	}

	d := &daemon{
		cfg:   cfg,
		store: deps.Store,
		dlv:   deps.Deliver,
		now:   now,
		clock: clock,
		gate:  newAmbientGate(cfg),
		class: classifierClient(cfg),
		wait:  wait,
	}

	// Reconnect catch-up: when the connection drops and reconnects, the
	// messages missed in the gap are replayed from history (the cursor
	// bounds the window). The hook runs in its own goroutine, off the
	// client's read loop.
	catchUp := func() { d.catchUp(ctx) }

	client := deps.Client
	if client == nil {
		var err error
		client, err = apronws.Connect(ctx, apronws.Options{URL: cfg.URL, Token: cfg.Token, OnReconnect: catchUp})
		if err != nil {
			return err
		}
		defer client.Close()
		// The connection is authenticated at this point, so pushes for the
		// bot's rooms arrive from here on. Operators and scripts wait on
		// this line to know the daemon is live.
		log.Printf("daemon: connected to %s", cfg.URL)
	} else {
		client.SetOnReconnect(catchUp)
	}
	d.client = client
	d.hist = newHistoryCache(client)

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
	return d.flushQueue(ctx)
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
// policy weight; room updates and activities are ignored. procMu is
// held only across the cursor check-then-act dedupe, evaluation, and
// enqueue; delivery (the queue flush and shared callbacks) runs after
// the lock is released so a slow deliverer cannot stall live
// processing or catch-up.
func (d *daemon) handleRecord(ctx context.Context, rec apronws.Record, subscribe bool) {
	if rec.Kind != apronws.KindMessage || rec.Message == nil {
		return
	}
	m := rec.Message
	d.procMu.Lock()
	deliveries, flush := d.processRecord(ctx, m, subscribe)
	d.procMu.Unlock()
	for _, h := range deliveries {
		d.deliverShared(ctx, h)
	}
	if flush {
		d.flushQueue(ctx)
	}
}

// processRecord runs one message's cursor check-then-act dedupe,
// evaluation, and enqueue; it must be called with procMu held. It
// returns the shared-mode deliveries owed (the caller delivers them
// after releasing procMu) and whether a queue flush is owed (a
// resident wake was enqueued).
func (d *daemon) processRecord(ctx context.Context, m *apronws.Message, subscribe bool) (deliveries []sharedHit, flush bool) {
	// Dedupe against the room cursor: a replayed message (catch-up) or a
	// duplicate push at or below the cursor is already evaluated. The
	// SetCursor max-upsert below advances the dedup line for both the
	// live loop and the catch-up goroutine. LogID is a global sequence,
	// so one per-room cursor suffices.
	cursor, err := d.store.GetCursor(ctx, m.RoomID)
	if err != nil {
		log.Printf("daemon: get cursor %s: %v", m.RoomID, err)
	} else if int64(m.LogID) <= cursor {
		return nil, false
	}
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
		return nil, false
	}
	// The bot's own posts arm thread subscriptions and are never
	// evaluated against the policy: no self-wake loops.
	if m.From.UserID == d.cfg.Self.UserID {
		if subscribe && m.ReplyTo != nil {
			d.subscribeOwnThread(ctx, m)
		}
		return nil, false
	}
	if d.cfg.Mode == ModeShared {
		return d.handleSharedLocked(ctx, m), false
	}
	return nil, d.handleResidentLocked(ctx, m)
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

// handleResidentLocked runs the full policy for one message and, on a
// wake, enqueues it for durability and removes fired once-watches. It
// must be called with procMu held; the caller flushes the queue after
// releasing the lock so a slow delivery cannot stall live processing.
// It reports whether a wake was enqueued (a flush is owed).
func (d *daemon) handleResidentLocked(ctx context.Context, m *apronws.Message) bool {
	msg := mapMsg(m)
	watches := d.roomWatches(ctx, m.RoomID)
	var resident []store.Watch
	for _, w := range watches {
		if d.ownsWatch(w.Owner) {
			resident = append(resident, w)
		}
	}
	dec := wake.Evaluate(ctx, wake.In{
		Msg:     msg,
		Self:    d.cfg.Self,
		Peers:   d.cfg.Peers,
		Watches: resident,
		History: func(ctx context.Context, messageID string) (string, bool, error) {
			return d.lookupAuthor(ctx, m.RoomID, messageID)
		},
		Classifier: d.class,
		Gate:       d.gate,
		Clock:      d.clock,
	})
	if !dec.Wake {
		return false
	}
	env := BuildEnvelope(dec, msg)
	env.ImageURL = firstImageURL(m)
	payload, err := encodePayload(env)
	if err != nil {
		log.Printf("daemon: encode wake: %v", err)
		return false
	}
	if _, err := d.store.EnqueueWake(ctx, store.Wake{Agent: d.cfg.Agent, Payload: payload}, d.now()); err != nil {
		log.Printf("daemon: enqueue wake: %v", err)
		return false
	}
	for _, id := range dec.RemoveWatchIDs {
		if err := d.store.DeleteWatch(ctx, id); err != nil {
			log.Printf("daemon: delete watch %s: %v", id, err)
		}
	}
	return true
}

// flushQueue delivers and removes every queued wake for this agent.
// Callers may overlap (the live loop's flush and the catch-up
// goroutine's flush run concurrently now that delivery is outside
// procMu): the store's dequeue is a transactional fetch+delete, so a
// wake is still delivered at most once; wake ordering across
// concurrent flushes is best-effort. A dequeue error is logged and
// returned; the caller decides whether it is fatal (the startup drain
// propagates it, the live loop does not).
func (d *daemon) flushQueue(ctx context.Context) error {
	wakes, err := d.store.DequeueWakes(ctx, d.cfg.Agent)
	if err != nil {
		log.Printf("daemon: dequeue wakes: %v", err)
		return err
	}
	for _, w := range wakes {
		if err := d.dlv.Deliver(ctx, w.Agent, decodePayload(w.Payload)); err != nil {
			log.Printf("daemon: deliver wake %d: %v", w.ID, err)
		}
	}
	return nil
}

// lookupAuthor resolves the reply target's author, retrying a failed
// History lookup up to 3 times (1-second pause between attempts). The
// room cursor has already advanced, so catch-up will not replay the
// message; a lost lookup loses the reply wake (at-most-once,
// documented). The bounded retry narrows the window.
func (d *daemon) lookupAuthor(ctx context.Context, roomID, messageID string) (string, bool, error) {
	const maxAttempts = 4
	var author string
	var ok bool
	var err error
	for attempt := 1; ; attempt++ {
		author, ok, err = d.hist.lookup(ctx, roomID, messageID)
		if err == nil || attempt == maxAttempts {
			break
		}
		d.wait(time.Second)
	}
	if err != nil {
		log.Printf("daemon: history lookup for %s failed after %d attempts: %v", messageID, maxAttempts, err)
	}
	return author, ok, err
}

// sharedHit is one shared-mode watch hit ready for delivery.
type sharedHit struct {
	owner string
	env   Envelope
	cmd   []string
}

// handleSharedLocked evaluates the shared policy for one message and
// returns the watch hits owed a delivery; the caller delivers them
// after releasing procMu so a slow callback cannot stall live
// processing. It must be called with procMu held. No mention/reply/
// ambient evaluation, no durable queue: a delivery failure is logged
// and dropped, because the transient run the wake belonged to may be
// gone. Only watches whose owner has a delivery callback are evaluated:
// a resident-owned watch has no callback and must not be fired (or its
// once-watch consumed) by a shared daemon.
func (d *daemon) handleSharedLocked(ctx context.Context, m *apronws.Message) []sharedHit {
	msg := mapMsg(m)
	var watches []store.Watch
	for _, w := range d.roomWatches(ctx, m.RoomID) {
		if len(d.ownerCommand(ctx, w.Owner)) > 0 {
			watches = append(watches, w)
		}
	}
	dec := wake.Evaluate(ctx, wake.In{
		Msg:     msg,
		Self:    d.cfg.Self,
		Peers:   d.cfg.Peers,
		Watches: watches,
		Clock:   d.clock,
	})
	// Mention/reply/ambient decisions carry no watch hits here; only
	// watch fires are acted on.
	var out []sharedHit
	for _, hit := range dec.Watches {
		hitDec := wake.Decision{
			Wake:    true,
			Kind:    wake.KindWatch,
			Reason:  hit.Note,
			Watches: []wake.WatchHit{hit},
		}
		env := BuildEnvelope(hitDec, msg)
		env.ImageURL = firstImageURL(m)
		out = append(out, sharedHit{owner: hit.Owner, env: env, cmd: d.ownerCommand(ctx, hit.Owner)})
	}
	for _, id := range dec.RemoveWatchIDs {
		if err := d.store.DeleteWatch(ctx, id); err != nil {
			log.Printf("daemon: delete watch %s: %v", id, err)
		}
	}
	return out
}

// deliverShared delivers one shared-mode hit: the owner's callback when
// one is set, else the injected deliverer. A failure is logged and
// dropped (the transient run may be gone; at-most-once).
func (d *daemon) deliverShared(ctx context.Context, h sharedHit) {
	if len(h.cmd) > 0 {
		if err := (&execDeliverer{cmd: h.cmd}).Deliver(ctx, h.owner, h.env); err != nil {
			log.Printf("daemon: deliver watch wake to %s: %v", h.owner, err)
		}
		return
	}
	if err := d.dlv.Deliver(ctx, h.owner, h.env); err != nil {
		log.Printf("daemon: deliver watch wake to %s: %v", h.owner, err)
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
// with owner: watch.DefaultOwner (the default) and the daemon's own
// agent are kept; anything else belongs to a shared-mode transient with
// its own callback and must not be consumed here.
func (d *daemon) ownsWatch(owner string) bool {
	return owner == watch.DefaultOwner || owner == d.cfg.Agent
}

// expireDue delivers expiry envelopes for every due watch and deletes
// the watch: expiry is a wake the agent must see to re-arm. In resident
// mode the envelope routes through the durable queue under cfg.Agent
// (matching the message path) and foreign-owner watches are skipped; an
// enqueue failure skips the watch for this sweep. In shared mode owners
// without a delivery callback are skipped (the same rule as the message
// path), and a failed delivery keeps the watch so the expiry re-fires
// on the next sweep instead of being lost.
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
			// Shared mode delivers only to callback owners (the same
			// rule as the message path); a failed delivery keeps the
			// watch so the expiry re-fires on the next sweep instead
			// of being lost.
			cmd := d.ownerCommand(ctx, w.Owner)
			if len(cmd) == 0 {
				continue
			}
			if err := (&execDeliverer{cmd: cmd}).Deliver(ctx, w.Owner, env); err != nil {
				log.Printf("daemon: deliver expiry to %s: %v", w.Owner, err)
				continue
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
