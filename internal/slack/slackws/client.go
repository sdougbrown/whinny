// Package slackws is the Slack Socket Mode client: it opens the socket
// with the app-level token, acks every payload, and delivers decoded
// events. The Web API half lives in the parent package slack.
package slackws

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/sdougbrown/whinny/internal/slack"
)

// Event is one inbound Socket Mode event.
type Event struct {
	// Type is the inner event's type, e.g. "message" or "app_mention".
	Type string
	// Payload is the inner event's JSON.
	Payload json.RawMessage
	// EnvelopeID is the Socket Mode envelope id.
	EnvelopeID string
}

// Options configures Connect.
type Options struct {
	// AppToken is the app-level token (xapp-…) for apps.connections.open.
	AppToken string
	// BaseURL overrides the Web API base URL (tests); the returned ws://
	// URL is dialed directly.
	BaseURL string
	// HTTPClient overrides the HTTP client used for
	// apps.connections.open (tests).
	HTTPClient *http.Client
	// OnReconnect, if set, is called on each successful reconnect after
	// the initial connect. It runs in its own goroutine, off the
	// client's read loop; keep it short and non-blocking.
	OnReconnect func()
}

// Client is a Slack Socket Mode client.
//
// The context passed to Connect bounds the initial handshake and then
// controls the client's lifetime: when it is done, the client stops
// reconnecting and closes the connection.
type Client struct {
	opts Options
	api  *slack.Client

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu          sync.Mutex
	onReconnect func()
	conn        *websocket.Conn

	// dedupe tracks recently seen envelope ids: Slack resends payloads
	// left un-acked, so duplicates are dropped after acking.
	seen    map[string]struct{}
	ring    []string
	ringPos int

	records chan Event
}

// dedupeSize is how many envelope ids the client remembers.
const dedupeSize = 256

// Connect opens a Socket Mode connection: apps.connections.open with the
// app-level token, then a dial of the returned WSS URL, then the read
// loop. Reconnects with backoff (1s to 30s) on disconnect or read error.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	var apiOpts []slack.ClientOption
	if opts.BaseURL != "" {
		apiOpts = append(apiOpts, slack.WithBaseURL(opts.BaseURL))
	}
	if opts.HTTPClient != nil {
		apiOpts = append(apiOpts, slack.WithHTTPClient(opts.HTTPClient))
	}
	c := &Client{
		opts:    opts,
		api:     slack.NewClient(opts.AppToken, apiOpts...),
		seen:    make(map[string]struct{}, dedupeSize),
		ring:    make([]string, dedupeSize),
		records: make(chan Event, 256),
	}
	c.mu.Lock()
	c.onReconnect = opts.OnReconnect
	c.mu.Unlock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})

	url, err := c.api.ConnectionsOpen(ctx)
	if err != nil {
		c.cancel()
		return nil, err
	}
	if err := c.dial(ctx, url); err != nil {
		c.cancel()
		return nil, err
	}
	go c.run()
	return c, nil
}

// Records returns the channel of decoded inbound events. The channel is
// closed when the client stops.
func (c *Client) Records() <-chan Event {
	return c.records
}

// SetOnReconnect sets (or clears) the reconnect hook. It is safe to call
// after Connect: the hook is read under the same lock the run loop uses,
// so a hook set after the initial connect is picked up by the next
// reconnect without racing the read loop.
func (c *Client) SetOnReconnect(fn func()) {
	c.mu.Lock()
	c.onReconnect = fn
	c.mu.Unlock()
}

// Close stops the client: it stops reconnecting and closes the
// connection. It is idempotent.
func (c *Client) Close() {
	c.cancel()
	<-c.done
}

// dial establishes the WebSocket connection.
func (c *Client) dial(ctx context.Context, url string) error {
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(hctx, url, nil)
	if err != nil {
		return err
	}
	conn.SetReadLimit(-1)
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

// run drives the read loop and reconnects with exponential backoff (1s
// to 30s) with a fresh URL until the client's context is done.
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
		url, err := c.api.ConnectionsOpen(c.ctx)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		if err := c.dial(c.ctx, url); err != nil {
			if c.ctx.Err() != nil {
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		c.mu.Lock()
		hook := c.onReconnect
		c.mu.Unlock()
		if hook != nil {
			go hook()
		}
	}
}

// readLoop reads envelopes until the connection dies or the client's
// context is done. It acks every payload on receipt and delivers
// events_api events; interactive and slash_commands payloads are acked
// and ignored.
func (c *Client) readLoop() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return
	}
	for {
		typ, data, err := conn.Read(c.ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var env struct {
			Type       string          `json:"type"`
			EnvelopeID string          `json:"envelope_id"`
			Payload    json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}
		switch env.Type {
		case "disconnect":
			return
		case "events_api", "interactive", "slash_commands":
			c.ack(conn, env.EnvelopeID)
		default:
			continue
		}
		if env.Type != "events_api" || env.EnvelopeID == "" || c.duplicate(env.EnvelopeID) {
			continue
		}
		var probe struct {
			Type  string          `json:"type"`
			Event json.RawMessage `json:"event"`
		}
		if err := json.Unmarshal(env.Payload, &probe); err != nil {
			continue
		}
		// A real events_api payload is a wrapper:
		// {"type":"event_callback","event":{...}} — the event's own
		// type lives under "event". Unwrap it; a malformed wrapper
		// (no inner type) is dropped. Payloads without the wrapper
		// (bare events, as test fakes may emit) pass through.
		evType, payload := probe.Type, env.Payload
		if probe.Type == "event_callback" {
			var inner struct {
				Type string `json:"type"`
			}
			if len(probe.Event) == 0 || json.Unmarshal(probe.Event, &inner) != nil || inner.Type == "" {
				continue
			}
			evType, payload = inner.Type, probe.Event
		}
		select {
		case c.records <- Event{Type: evType, Payload: payload, EnvelopeID: env.EnvelopeID}:
		case <-c.ctx.Done():
			return
		}
	}
}

// ack writes the Socket Mode ack for an envelope.
func (c *Client) ack(conn *websocket.Conn, envelopeID string) {
	if envelopeID == "" {
		return
	}
	data, _ := json.Marshal(map[string]string{"type": "ack", "envelope_id": envelopeID})
	_ = conn.Write(c.ctx, websocket.MessageText, data)
}

// duplicate reports whether the envelope id was seen before, recording it
// if not.
func (c *Client) duplicate(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[id]; ok {
		return true
	}
	if old := c.ring[c.ringPos]; old != "" {
		delete(c.seen, old)
	}
	c.ring[c.ringPos] = id
	c.ringPos = (c.ringPos + 1) % dedupeSize
	c.seen[id] = struct{}{}
	return false
}

// teardown closes the current connection.
func (c *Client) teardown() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		conn.CloseNow()
	}
}
