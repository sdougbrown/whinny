package apronws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Options configures a Client.
type Options struct {
	// URL is the apron WebSocket endpoint. If it has no path, "/ws" is
	// appended.
	URL string
	// Token is the auth token, sent with the "token" scheme.
	Token string
	// OnReconnect, if set, is called on each successful reconnect after
	// the initial connect. It runs in its own goroutine, off the client's
	// read loop; keep it short and non-blocking.
	OnReconnect func()
}

// Client is an apron WebSocket client.
//
// The context passed to Connect bounds the initial handshake and then
// controls the client's lifetime: when it is done, the client stops
// reconnecting and closes the connection.
//
// The client serializes auth: no request is sent until auth completes.
// While a reconnect is in flight, Send returns ErrNotAuthenticated.
type Client struct {
	opts Options
	url  string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// onReconnect, if set, is called on each successful reconnect after
	// the initial connect. It runs in its own goroutine, off the client's
	// read loop; keep it short and non-blocking.
	onReconnect func()

	// http is the HTTP client the one HTTP fetch the client makes (the
	// ext:emoji catalog) uses; nil uses http.DefaultClient.
	httpDoer interface {
		Do(*http.Request) (*http.Response, error)
	}

	mu     sync.Mutex
	live   *live
	authed bool
	// token is the server-issued token from the last auth; it is preferred
	// over opts.Token on (re)auth. Empty until the first auth succeeds.
	token   string
	server  ServerInfo
	pending map[string]*pending
	// idPrefix distinguishes the clients of one process sharing an
	// apron account. The server deduplicates request ids per user
	// across connections (§1.2), and id-suffix uniqueness cannot reach
	// across processes, so Connect seeds idPrefix with the process's
	// start time: two whinny processes (the daemon and a CLI command)
	// never reuse one id with a different request fingerprint.
	idPrefix uint64
	idSeq    uint64

	records chan Record
}

// live is the state of one connection: the socket, its keepalive, and the
// pong signal.
type live struct {
	conn   *websocket.Conn
	cancel context.CancelFunc
	pong   chan struct{}
}

// Connect dials the apron endpoint, reads and records the server push,
// authenticates with the token, and starts the background read, keepalive,
// and reconnect loops.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	c := &Client{
		opts:        opts,
		url:         wsURL(opts.URL),
		onReconnect: opts.OnReconnect,
		pending:     make(map[string]*pending),
		idPrefix:    uint64(time.Now().UnixNano()) & 0x7fff_ffff_ffff,
		records:     make(chan Record, 256),
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})
	if err := c.handshake(ctx); err != nil {
		c.cancel()
		return nil, err
	}
	go c.run()
	return c, nil
}

// SetOnReconnect sets the hook called on each successful reconnect after
// the initial connect. It runs in its own goroutine, off the client's
// read loop; keep it short and non-blocking. It may be called while the
// client is running.
func (c *Client) SetOnReconnect(fn func()) {
	c.mu.Lock()
	c.onReconnect = fn
	c.mu.Unlock()
}

// Close stops the client: it stops reconnecting, closes the connection, and
// fails in-flight requests. It is idempotent.
func (c *Client) Close() {
	c.cancel()
	<-c.done
}

// Records returns the channel of decoded inbound pushes. The channel is
// closed when the client stops. A slow consumer backpressures the
// connection's read loop.
func (c *Client) Records() <-chan Record {
	return c.records
}

// Server returns the recorded server push.
func (c *Client) Server() ServerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server
}

// Capabilities returns the server frame's capability list (§3.1).
func (c *Client) Capabilities() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server.Capabilities
}

// HasCapability reports whether the server offered the named capability.
func (c *Client) HasCapability(name string) bool {
	for _, cap := range c.Capabilities() {
		if cap == name {
			return true
		}
	}
	return false
}

// EmojiCatalog fetches the custom emoji catalog (ext:emoji): a JSON
// object keyed by :name:, each entry {url, width, height, alt}. When
// the server does not offer ext:emoji (no section in the server frame's
// ext), it returns an empty catalog and nil.
func (c *Client) EmojiCatalog(ctx context.Context) (map[string]EmojiEntry, error) {
	ext := func() json.RawMessage {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.server.Ext
	}()
	var frame struct {
		Emoji *struct {
			CatalogURL string `json:"catalog_url"`
		} `json:"emoji"`
	}
	if len(ext) > 0 && json.Unmarshal(ext, &frame) == nil && frame.Emoji != nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, frame.Emoji.CatalogURL, nil)
		if err != nil {
			return nil, err
		}
		doer := c.httpDoer
		if doer == nil {
			doer = http.DefaultClient
		}
		resp, err := doer.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("emoji catalog: http %d", resp.StatusCode)
		}
		var catalog map[string]EmojiEntry
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&catalog); err != nil {
			return nil, err
		}
		return catalog, nil
	}
	return map[string]EmojiEntry{}, nil
}

// EmojiEntry is one custom emoji's catalog entry.
type EmojiEntry struct {
	URL    string `json:"url"`
	Width  int    `json:"width,omitempty"`
	Height int    `json:"height,omitempty"`
	Alt    string `json:"alt,omitempty"`
}

// Send issues a request and waits for its reply. The connection must be
// authenticated; before the initial auth completes, or while a reconnect is
// in flight, Send returns ErrNotAuthenticated without sending anything.
// Concurrent requests may be in flight.
func (c *Client) Send(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	if !c.authed || c.live == nil {
		c.mu.Unlock()
		return nil, ErrNotAuthenticated
	}
	conn := c.live.conn
	id := c.nextID()
	p := &pending{ch: make(chan reply, 1)}
	c.pending[id] = p
	c.mu.Unlock()

	f := wireFrame{ID: id, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			c.removePending(id)
			return nil, err
		}
		f.Params = raw
	}
	if err := c.writeFrame(ctx, conn, f); err != nil {
		c.removePending(id)
		return nil, err
	}
	select {
	case r := <-p.ch:
		if r.err != nil {
			return nil, r.err
		}
		return r.result, nil
	case <-ctx.Done():
		c.removePending(id)
		return nil, ctx.Err()
	}
}

// Ping sends an application-level ping notification. The server answers
// with a pong push, which the keepalive uses for liveness.
func (c *Client) Ping(ctx context.Context) error {
	c.mu.Lock()
	if !c.authed || c.live == nil {
		c.mu.Unlock()
		return ErrNotAuthenticated
	}
	conn := c.live.conn
	c.mu.Unlock()
	return c.writeFrame(ctx, conn, wireFrame{Method: "ping"})
}

// PostParams is the params for the "message" request. The room is always
// explicit; an absent ReplyTo creates a new message.
type PostParams struct {
	RoomID  string   `json:"room_id"`
	Body    Body     `json:"body"`
	ReplyTo *ReplyTo `json:"reply_to,omitempty"`
}

// PostResult is the "message" request's result. Raw carries the full
// result object.
type PostResult struct {
	MessageID string `json:"message_id"`
	Raw       json.RawMessage
}

// PostMessage posts a message to a room.
func (c *Client) PostMessage(ctx context.Context, p PostParams) (PostResult, error) {
	raw, err := c.Send(ctx, "message", p)
	if err != nil {
		return PostResult{}, err
	}
	var r PostResult
	if err := json.Unmarshal(raw, &r); err != nil {
		return PostResult{}, err
	}
	r.Raw = raw
	return r, nil
}

// HistoryParams is the params for the "history" request.
type HistoryParams struct {
	RoomID string `json:"room_id,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// HistoryResult is the "history" request's result. Raw carries the full
// result object. Reactions holds the log's reaction records (§4.7),
// oldest first. Each record carries one author's complete set at that
// log position; an empty set is that author's clear.
type HistoryResult struct {
	Messages    []Message         `json:"messages"`
	Reactions   []ReactionsRecord `json:"reactions,omitempty"`
	Rooms       json.RawMessage   `json:"rooms,omitempty"`
	LatestLogID LogID             `json:"latest_log_id"`
	Raw         json.RawMessage
}

// History fetches message history, optionally for one room.
func (c *Client) History(ctx context.Context, p HistoryParams) (HistoryResult, error) {
	raw, err := c.Send(ctx, "history", p)
	if err != nil {
		return HistoryResult{}, err
	}
	var r HistoryResult
	if err := json.Unmarshal(raw, &r); err != nil {
		return HistoryResult{}, err
	}
	r.Raw = raw
	return r, nil
}

// SetReactionsParams is the params for the "reactions" request.
type SetReactionsParams struct {
	MessageID string   `json:"message_id"`
	Emojis    []string `json:"emojis"`
}

// SetReactions sets the caller's complete emoji set on one message
// (§4.7): an empty slice clears, duplicates collapse, and the server
// qualifies entries so ones differing only in U+FE0F are the same.
func (c *Client) SetReactions(ctx context.Context, messageID string, emojis []string) error {
	_, err := c.Send(ctx, "reactions", SetReactionsParams{MessageID: messageID, Emojis: emojis})
	return err
}

// RoomListParams is the params for the "room_list" request.
type RoomListParams struct {
	Filter      string          `json:"filter,omitempty"`
	Members     json.RawMessage `json:"members,omitempty"`
	LatestLogID LogID           `json:"latest_log_id,omitempty"`
}

// Room is a room as reported by room_list and room_set. Fields the
// protocol does not name are dropped.
type Room struct {
	ID           string `json:"room_id"`
	Title        string `json:"title,omitempty"`
	ParentRoomID string `json:"parent_room_id,omitempty"`
}

// RoomListResult is the "room_list" request's result. Raw carries the full
// result object.
type RoomListResult struct {
	Joined    []Room `json:"joined"`
	NotJoined []Room `json:"not_joined,omitempty"`
	Raw       json.RawMessage
}

// RoomList lists rooms, optionally filtered.
func (c *Client) RoomList(ctx context.Context, p RoomListParams) (RoomListResult, error) {
	raw, err := c.Send(ctx, "room_list", p)
	if err != nil {
		return RoomListResult{}, err
	}
	var r RoomListResult
	if err := json.Unmarshal(raw, &r); err != nil {
		return RoomListResult{}, err
	}
	r.Raw = raw
	return r, nil
}

// CreateThreadParams is the params for creating a thread: a room_set with
// no room_id.
type CreateThreadParams struct {
	ParentRoomID string `json:"parent_room_id"`
	Title        string `json:"title,omitempty"`
}

// CreateThread creates a thread under ParentRoomID and returns the new
// room.
func (c *Client) CreateThread(ctx context.Context, p CreateThreadParams) (Room, error) {
	raw, err := c.Send(ctx, "room_set", p)
	if err != nil {
		return Room{}, err
	}
	var r Room
	if err := json.Unmarshal(raw, &r); err != nil {
		return Room{}, err
	}
	return r, nil
}

// JoinRoom joins a room.
func (c *Client) JoinRoom(ctx context.Context, roomID string) error {
	_, err := c.Send(ctx, "room_join", roomIDParams{RoomID: roomID})
	return err
}

// LeaveRoom leaves a room.
func (c *Client) LeaveRoom(ctx context.Context, roomID string) error {
	_, err := c.Send(ctx, "room_leave", roomIDParams{RoomID: roomID})
	return err
}

// SetMute sets the mute state. An empty roomID mutes globally; otherwise
// the mute applies to the room.
func (c *Client) SetMute(ctx context.Context, muted bool, roomID string) error {
	_, err := c.Send(ctx, "status", struct {
		Mute   bool   `json:"mute"`
		RoomID string `json:"room_id,omitempty"`
	}{Mute: muted, RoomID: roomID})
	return err
}

type roomIDParams struct {
	RoomID string `json:"room_id"`
}

// wireFrame is one I-JSON protocol frame. A request has ID, Method, and
// Params; a notification has only Method; a reply has ID and Result or
// Error; a push has Method and Data.
type wireFrame struct {
	ID     string          `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *wireError      `json:"error,omitempty"`
}

type authParams struct {
	Scheme string `json:"scheme"`
	Token  string `json:"token"`
}

type authResult struct {
	You   json.RawMessage `json:"you"`
	Token string          `json:"token"`
}

type reply struct {
	result json.RawMessage
	err    error
}

type pending struct {
	ch   chan reply
	once sync.Once
}

func (p *pending) deliver(r reply) {
	p.once.Do(func() { p.ch <- r })
}

// authToken returns the token to send with the next (re)auth: the
// server-issued token when present, else the configured token.
func (c *Client) authToken() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" {
		return c.token
	}
	return c.opts.Token
}

// clampPingInterval clamps the server-suggested keepalive interval to the
// 5s-10m band: below 5s clamps to 5s, above 10m clamps to 10m, and zero
// (unset) uses the 30s default.
func clampPingInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return 30 * time.Second
	}
	if d < 5*time.Second {
		return 5 * time.Second
	}
	if d > 10*time.Minute {
		return 10 * time.Minute
	}
	return d
}

// wsURL appends "/ws" when the URL has no path, per the apron convention.
func wsURL(u string) string {
	p, err := url.Parse(u)
	if err != nil || p.Path == "" || p.Path == "/" {
		return strings.TrimSuffix(u, "/") + "/ws"
	}
	return u
}

// handshake dials, waits for the server push, and completes the auth
// request. The auth reply is the last frame it consumes; the read loop
// starts after it returns. The pre-auth phase is bounded by an internal
// deadline so a wedged server feeds the backoff loop instead of hanging.
func (c *Client) handshake(ctx context.Context) error {
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(hctx, c.url, nil)
	if err != nil {
		return err
	}
	// History replies and pushes can exceed coder/websocket's 32KB default
	// per-message read limit; unlimit it.
	conn.SetReadLimit(-1)
	lv := &live{conn: conn, pong: make(chan struct{}, 1)}
	c.mu.Lock()
	c.live = lv
	c.mu.Unlock()

	for {
		typ, data, err := conn.Read(hctx)
		if err != nil {
			c.abortConn()
			return err
		}
		if typ != websocket.MessageText {
			continue
		}
		var f wireFrame
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		if f.ID != "" {
			c.deliverReply(f)
			continue
		}
		if f.Method == "server" {
			info, err := decodeServerInfo(f.Params)
			if err != nil {
				c.abortConn()
				return err
			}
			c.mu.Lock()
			c.server = info
			c.mu.Unlock()
			break
		}
		c.dispatchPush(f)
	}

	id := c.nextID()
	p := &pending{ch: make(chan reply, 1)}
	c.mu.Lock()
	c.pending[id] = p
	c.mu.Unlock()
	params, _ := json.Marshal(authParams{Scheme: "token", Token: c.authToken()})
	if err := c.writeFrame(hctx, conn, wireFrame{ID: id, Method: "auth", Params: params}); err != nil {
		c.removePending(id)
		c.abortConn()
		return err
	}
	for {
		typ, data, err := conn.Read(hctx)
		if err != nil {
			c.removePending(id)
			c.abortConn()
			return err
		}
		if typ != websocket.MessageText {
			continue
		}
		var f wireFrame
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		if f.ID == "" {
			c.dispatchPush(f)
			continue
		}
		if f.ID != id {
			c.deliverReply(f)
			continue
		}
		c.removePending(id)
		if f.Error != nil {
			// The rotated token was rejected; fall back to opts.Token on the
			// next (re)auth attempt.
			c.mu.Lock()
			c.token = ""
			c.mu.Unlock()
			c.abortConn()
			return f.Error.clientError()
		}
		var ar authResult
		if err := json.Unmarshal(f.Result, &ar); err != nil {
			c.abortConn()
			return err
		}
		// The auth result's token is a freshly-issued token; tokens renew on
		// use (30-day validity), so prefer it over opts.Token on reconnect.
		c.mu.Lock()
		c.authed = true
		if ar.Token != "" {
			c.token = ar.Token
		}
		c.mu.Unlock()
		lv.cancel = c.startKeepalive(conn, lv.pong, c.server.Ping)
		return nil
	}
}

// run drives the read loop and reconnects with exponential backoff (1s to
// 30s) until the client's context is done.
func (c *Client) run() {
	defer close(c.records)
	defer close(c.done)
	backoff := time.Second
	for {
		c.readLoop()
		c.teardown()
		if c.ctx.Err() != nil {
			return
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(backoff):
		}
		if err := c.handshake(c.ctx); err != nil {
			if c.ctx.Err() != nil {
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		if hook := c.reconnectHook(); hook != nil {
			go hook()
		}
	}
}

// reconnectHook returns the current reconnect hook.
func (c *Client) reconnectHook() func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.onReconnect
}

// readLoop dispatches frames until the connection dies or the client's
// context is done.
func (c *Client) readLoop() {
	c.mu.Lock()
	lv := c.live
	c.mu.Unlock()
	if lv == nil {
		return
	}
	for {
		typ, data, err := lv.conn.Read(c.ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var f wireFrame
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		if f.ID != "" {
			c.deliverReply(f)
			continue
		}
		c.dispatchPush(f)
	}
}

// teardown closes the current connection, stops its keepalive, and fails
// in-flight requests.
func (c *Client) teardown() {
	c.mu.Lock()
	lv := c.live
	c.live = nil
	c.authed = false
	c.mu.Unlock()
	if lv != nil {
		lv.cancel()
		lv.conn.CloseNow()
	}
	err := ErrConnectionLost
	if c.ctx.Err() != nil {
		err = ErrClientClosed
	}
	c.failPending(err)
}

// abortConn closes and clears the current connection after a failed
// handshake.
func (c *Client) abortConn() {
	c.mu.Lock()
	lv := c.live
	c.live = nil
	c.mu.Unlock()
	if lv != nil {
		lv.conn.CloseNow()
	}
}

// startKeepalive sends an application-level ping on the server-suggested
// interval (30s when unset) and kills the connection if a pong is missed.
func (c *Client) startKeepalive(conn *websocket.Conn, pong chan struct{}, interval time.Duration) context.CancelFunc {
	ctx, cancel := context.WithCancel(c.ctx)
	interval = clampPingInterval(interval)
	go func() {
		defer cancel()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		deadline := time.NewTimer(2 * interval)
		defer deadline.Stop()
		resetDeadline := func() {
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}
			deadline.Reset(2 * interval)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = c.writeFrame(ctx, conn, wireFrame{Method: "ping"})
			case <-pong:
				resetDeadline()
			case <-deadline.C:
				conn.CloseNow()
				return
			}
		}
	}()
	return cancel
}

func (c *Client) writeFrame(ctx context.Context, conn *websocket.Conn, f wireFrame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

// nextID returns a per-client unique request id. Apron deduplicates
// requests per user across connections (§1.2): two clients sharing an
// account (the daemon and a CLI command) must not reuse one id with a
// different fingerprint, so the id carries the per-process prefix.
func (c *Client) nextID() string {
	return fmt.Sprintf("w%d-%d", c.idPrefix, atomic.AddUint64(&c.idSeq, 1))
}

func (c *Client) removePending(id string) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Client) failPending(err error) {
	c.mu.Lock()
	ps := make([]*pending, 0, len(c.pending))
	for id, p := range c.pending {
		ps = append(ps, p)
		delete(c.pending, id)
	}
	c.mu.Unlock()
	for _, p := range ps {
		p.deliver(reply{err: err})
	}
}

func (c *Client) deliverReply(f wireFrame) {
	c.mu.Lock()
	p := c.pending[f.ID]
	delete(c.pending, f.ID)
	c.mu.Unlock()
	if p == nil {
		return
	}
	r := reply{result: f.Result}
	if f.Error != nil {
		r.err = f.Error.clientError()
	}
	p.deliver(r)
}

// dispatchPush decodes an inbound push and delivers it to Records. Unknown
// push methods are ignored, not errors.
func (c *Client) dispatchPush(f wireFrame) {
	if f.Method == "pong" {
		c.mu.Lock()
		lv := c.live
		c.mu.Unlock()
		if lv != nil {
			select {
			case lv.pong <- struct{}{}:
			default:
			}
		}
		return
	}
	rec, ok := decodePush(f.Method, f.Params)
	if !ok {
		return
	}
	select {
	case c.records <- rec:
	case <-c.ctx.Done():
	}
}
