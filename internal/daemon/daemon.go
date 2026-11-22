package daemon

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/slack"
	"github.com/sdougbrown/whinny/internal/slack/slackws"
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

// Deps carries the injected dependencies. Link may be set to run the
// daemon over any transport (tests inject a fake); otherwise Client may
// be set (wrapped in the apron adapter), and when both are nil Run
// connects through apronws itself and owns the client.
type Deps struct {
	Client  *apronws.Client
	Link    Link
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
	// SlackBaseURL, if set, overrides the Slack Web API base URL (tests
	// point it at a fake server). Empty uses the real API.
	SlackBaseURL string
	// SlackWSBaseURL, if set, overrides the Slack Socket Mode API base
	// URL (tests point it at a fake server). Empty uses the real API.
	SlackWSBaseURL string
}

// daemon is the run-loop state: one connected link, one store, one
// deliverer.
type daemon struct {
	cfg   Config
	store *store.Store
	link  Link
	dlv   Deliverer
	now   func() time.Time
	clock wake.Clock
	gate  *wake.AmbientGate
	class wake.Classifier
	hist  *historyCache
	wait  func(time.Duration)
	// procMu serializes record handling between the live loop and the
	// reconnect catch-up goroutine: the cursor dedupe is check-then-act
	// (GetCursor then SetCursor, with no store-side lock spanning both),
	// so an unlocked overlap lets one message evaluate twice. Held only
	// across the dedupe check, evaluation, and enqueue; delivery (the
	// queue flush and shared callbacks) runs after the lock is released
	// so a slow deliverer cannot stall live processing or catch-up.
	// Deliver must not re-enter handleEvent.
	procMu sync.Mutex
}

// Run drives the daemon until ctx is done: it validates the config
// (including the delivery backend, before any connection is attempted),
// builds the transport link (injected Link, injected apron client, or a
// fresh apron connection), drains stale queued wakes for its agent, then
// loops over inbound events and the expiry sweep. It returns nil on
// clean shutdown (ctx cancel or link close).
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
	// transport's read loop.
	catchUp := func() { d.catchUp(ctx) }

	switch {
	case deps.Link != nil:
		d.link = deps.Link
	case cfg.Transport == TransportSlack:
		apiOpts := []slack.ClientOption{}
		if deps.SlackBaseURL != "" {
			apiOpts = append(apiOpts, slack.WithBaseURL(deps.SlackBaseURL))
		}
		api := slack.NewClient(cfg.BotToken, apiOpts...)
		wsOpts := slackws.Options{AppToken: cfg.AppToken}
		if deps.SlackWSBaseURL != "" {
			wsOpts.BaseURL = deps.SlackWSBaseURL
		}
		ws, err := slackws.Connect(ctx, wsOpts)
		if err != nil {
			return err
		}
		defer ws.Close()
		link := newSlackLink(api, ws)
		// The bot's user id drives the self-post firebreak; resolve it
		// from the server when the config leaves it empty.
		if err := link.Identify(ctx); err != nil {
			return err
		}
		if cfg.Self.UserID == "" {
			cfg.Self.UserID = link.SelfUserID()
		}
		// The daemon struct holds its own copy of cfg (built before the
		// transport switch); carry the resolved id through so the policy
		// reads it.
		d.cfg.Self.UserID = cfg.Self.UserID
		d.link = link
		log.Printf("daemon: connected to slack as %s", link.SelfUserID())
	default:
		client := deps.Client
		if client == nil {
			var err error
			client, err = apronws.Connect(ctx, apronws.Options{URL: cfg.URL, Token: cfg.Token})
			if err != nil {
				return err
			}
			defer client.Close()
			// The connection is authenticated at this point, so pushes for
			// the bot's rooms arrive from here on. Operators and scripts
			// wait on this line to know the daemon is live.
			log.Printf("daemon: connected to %s", cfg.URL)
		}
		d.link = newApronLink(client)
	}
	d.hist = newHistoryCache(d.link)
	// A delete tombstone carries no body to evaluate, but its author is
	// recorded so a later reply to the deleted message resolves; the
	// adapter forwards tombstones to this callback. Slack's
	// message_deleted events carry no author, so the Slack link never
	// implements this hook: deleted-message authors are unresolvable
	// on Slack (a Slack-specific limitation; apron's tombstones do
	// feed the cache).
	if f, ok := d.link.(interface {
		SetTombstoneFeed(func(string, string, string))
	}); ok {
		f.SetTombstoneFeed(d.hist.feed)
	}
	// The reconnect hook is optional: an apron link forwards it to the
	// client; a link without the hook simply never replays on reconnect.
	if r, ok := d.link.(interface{ SetOnReconnect(func()) }); ok {
		r.SetOnReconnect(catchUp)
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

	events, err := d.link.Events(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C():
			d.expireDue(ctx)
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			d.handleEvent(ctx, ev, subscribe)
		}
	}
}

func errNilDep(name string) error {
	return &nilDepError{name: name}
}

type nilDepError struct{ name string }

func (e *nilDepError) Error() string { return "daemon: nil " + e.name + " in Deps" }

// newAmbientGate builds the gate for the ambient config, or nil when
// the gate is disabled. The gate is forced off for the Slack transport:
// Slack has no quiet/mute mapping, so ambient wakes there could not be
// silenced.
func newAmbientGate(cfg Config) *wake.AmbientGate {
	if cfg.Ambient.Endpoint == "" || cfg.Transport == TransportSlack {
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
	rooms, err := d.link.Rooms(ctx)
	if err != nil {
		log.Printf("daemon: catch-up room list: %v", err)
		return
	}
	subscribe := d.autoSubscribe(ctx)
	for _, room := range rooms {
		cursor, err := d.store.GetCursor(ctx, room.ID)
		if err != nil {
			log.Printf("daemon: catch-up cursor %s: %v", room.ID, err)
			continue
		}
		events, err := d.link.History(ctx, room.ID, cursor, catchUpLimit)
		if err != nil {
			log.Printf("daemon: catch-up history %s: %v", room.ID, err)
			continue
		}
		for _, ev := range events {
			d.handleEvent(ctx, ev, subscribe)
		}
	}
}

// handleEvent processes one inbound event. Two-phase resolution: the
// reply-author lookup (a network call with a bounded retry) runs
// outside procMu — holding the lock across it would stall the live
// loop and catch-up. The author is pre-resolved before the lock, only
// when the resident evaluation will actually need it: a non-self reply
// in resident mode that is not a duplicate (a shared daemon never
// wakes on replies, a self-post never wakes via reply, and a duplicate
// is skipped by the dedupe). procMu is then held only across the
// cursor check-then-act dedupe, evaluation, and enqueue; delivery (the
// queue flush and shared callbacks) runs after the lock is released so
// a slow deliverer cannot stall live processing or catch-up.
func (d *daemon) handleEvent(ctx context.Context, ev LinkEvent, subscribe bool) {
	if ev.Kind != KindMessage {
		return
	}
	// Pre-resolve the reply target's author outside procMu (see the
	// comment above), only when the resident evaluation will need it:
	// resident mode (a shared daemon never wakes on replies), a
	// non-self sender (a self-post never wakes via reply), and a
	// message past the room cursor (a duplicate is skipped by the
	// check-then-act under procMu; the cursor only moves forward, so
	// a message at or below it now is one at or below it then).
	var author string
	var authorOK bool
	var authorErr error
	if d.cfg.Mode == ModeResident && ev.Msg.ReplyTo != "" && ev.Msg.From.UserID != d.cfg.Self.UserID {
		cursor, err := d.store.GetCursor(ctx, ev.RoomID)
		if err != nil || ev.Cursor > cursor {
			author, authorOK, authorErr = d.lookupAuthor(ctx, ev.RoomID, ev.Msg.ReplyTo)
		}
	}
	d.procMu.Lock()
	deliveries, flush := d.processEvent(ctx, ev, subscribe, author, authorOK, authorErr)
	d.procMu.Unlock()
	for _, h := range deliveries {
		d.deliverShared(ctx, h)
	}
	if flush {
		d.flushQueue(ctx)
	}
}

// processEvent runs one event's cursor check-then-act dedupe,
// evaluation, and enqueue; it must be called with procMu held. The
// reply target's author is pre-resolved by the caller (outside
// procMu); processEvent passes it to the evaluation. It returns the
// shared-mode deliveries owed (the caller delivers them after
// releasing procMu) and whether a queue flush is owed (a resident wake
// was enqueued).
func (d *daemon) processEvent(ctx context.Context, ev LinkEvent, subscribe bool, author string, authorOK bool, authorErr error) (deliveries []sharedHit, flush bool) {
	m := ev.Msg
	// Dedupe against the room cursor: a replayed event (catch-up) or a
	// duplicate push at or below the cursor is already evaluated. The
	// SetCursor max-upsert below advances the dedup line for both the
	// live loop and the catch-up goroutine. The cursor is the
	// transport's monotonic position (apron log_id, Slack ts in
	// microseconds), so one per-room cursor suffices.
	cursor, err := d.store.GetCursor(ctx, ev.RoomID)
	if err != nil {
		log.Printf("daemon: get cursor %s: %v", ev.RoomID, err)
	} else if ev.Cursor <= cursor {
		return nil, false
	}
	// Feed the reply-author cache as events stream past, so a later
	// reply to this message resolves without a History call.
	d.hist.feed(m.MessageID, ev.RoomID, m.From.UserID)
	if err := d.store.SetCursor(ctx, ev.RoomID, ev.Cursor); err != nil {
		log.Printf("daemon: set cursor %s: %v", ev.RoomID, err)
	}
	// The bot's own posts arm thread subscriptions and are never
	// evaluated against the policy: no self-wake loops.
	if m.From.UserID == d.cfg.Self.UserID {
		if subscribe && m.ReplyTo != "" {
			d.subscribeOwnThread(ctx, ev.RoomID, m.ReplyTo)
		}
		return nil, false
	}
	if d.cfg.Mode == ModeShared {
		return d.handleSharedLocked(ctx, m, ev.ImageURL), false
	}
	return nil, d.handleResidentLocked(ctx, m, ev.ImageURL, author, authorOK, authorErr)
}

// subscribeOwnThread arms a subscription watch on the thread the bot
// just posted to: thread posts anchor at the parent message, so the
// event's ReplyTo is the thread root. Owner is this daemon's agent;
// TTL is one day.
func (d *daemon) subscribeOwnThread(ctx context.Context, roomID, threadID string) {
	_, err := d.store.AddWatch(ctx, store.Watch{
		RoomID:   roomID,
		Owner:    d.cfg.Agent,
		Kind:     watch.KindSubscription,
		ThreadID: threadID,
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
// The reply target's author is pre-resolved by the caller (outside
// procMu); the HistoryLookup returns that value without touching the
// network. It reports whether a wake was enqueued (a flush is owed).
func (d *daemon) handleResidentLocked(ctx context.Context, m wake.Msg, imageURL string, author string, authorOK bool, authorErr error) bool {
	msg := m
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
		// The author was pre-resolved outside procMu (see
		// handleEvent); this lookup returns that value without
		// touching the network.
		History: func(ctx context.Context, messageID string) (string, bool, error) {
			return author, authorOK, authorErr
		},
		Classifier: d.class,
		Gate:       d.gate,
		Clock:      d.clock,
	})
	if !dec.Wake {
		return false
	}
	env := BuildEnvelope(dec, msg)
	env.ImageURL = imageURL
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

// lookupAuthor resolves the reply target's author, making up to
// maxAttempts History calls (1 initial + 3 retries, a 1-second pause
// between attempts, each attempt bounded by a 5-second timeout). It is
// a network call and runs outside procMu (see handleEvent): holding
// the lock across the retry would stall the live loop and catch-up.
// The room cursor advances
// after the lookup (regardless of its outcome), so catch-up will not
// replay the message; a lost lookup loses the reply wake (at-most-once,
// documented). The bounded retry narrows the window.
func (d *daemon) lookupAuthor(ctx context.Context, roomID, messageID string) (string, bool, error) {
	const maxAttempts = 4
	var author string
	var ok bool
	var err error
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		author, ok, err = d.hist.lookup(attemptCtx, roomID, messageID)
		cancel()
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
func (d *daemon) handleSharedLocked(ctx context.Context, m wake.Msg, imageURL string) []sharedHit {
	msg := m
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
		env.ImageURL = imageURL
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
