package daemon

import (
	"fmt"
	"time"

	"github.com/sdougbrown/whinny/internal/wake"
)

// Modes.
const (
	// ModeResident runs the full wake policy for one resident identity,
	// backed by the durable wake queue.
	ModeResident = "resident"
	// ModeShared evaluates watches only and fires hits straight to their
	// owners' delivery callbacks, with no queue and no redelivery.
	ModeShared = "shared"
)

// Transports.
const (
	// TransportApron runs the daemon over an apron websocket (default).
	TransportApron = "apron"
	// TransportSlack runs the daemon over Slack Socket Mode.
	TransportSlack = "slack"
)

// Delivery backends.
const (
	// DeliverExec spawns Config.ExecCmd per wake with the envelope on
	// stdin. DeliverResume uses the same spawn shape; the caller's
	// command owns the session semantics, so the daemon is
	// phase-agnostic.
	DeliverExec   = "exec"
	DeliverResume = "resume"
	// DeliverSocket POSTs the envelope as JSON to Config.SocketURL.
	DeliverSocket = "socket"
	// DeliverFeed writes FeedRecord JSON lines to the daemon's stdout
	// (the process runs under poo's feed runner).
	DeliverFeed = "feed"
)

// Meta key prefixes and values in the store's meta table.
const (
	// MetaDeliveryPrefix prefixes an owner's delivery callback: the meta
	// key "delivery:<owner>" holds a command string (shell-split) that
	// receives the envelope on stdin. Shared mode routes each watch
	// hit's wake through its owner's callback; a later chunk's CLI
	// send-path writes the same key.
	MetaDeliveryPrefix = "delivery:"
	// MetaAutoSubscribe disables self-post auto-subscription when set to
	// "off": the daemon then no longer arms a subscription watch under
	// its own thread posts.
	MetaAutoSubscribe    = "auto_subscribe"
	MetaAutoSubscribeOff = "off"
)

// Config is the daemon's resolved configuration.
type Config struct {
	// URL and Token are the apron endpoint and auth token.
	URL, Token string
	// Transport selects the room transport: "apron" (default) or
	// "slack". Slack reads BotToken/AppToken; the CLI resolves the
	// same vocabulary from $WHINNY_TRANSPORT.
	Transport string
	// BotToken and AppToken are the Slack bot token (xoxb-) and
	// app-level token (xapp-) for the slack transport.
	BotToken, AppToken string
	// DBPath is the state database path (informational: the store is
	// injected through Deps).
	DBPath string
	// Agent is the resident identity: the wake_queue agent this daemon
	// drains, and the envelope attribution for self-post subscriptions.
	Agent string
	// Mode is "resident" (default) or "shared".
	Mode string
	// Deliver selects the backend: "exec" (default), "resume", "socket",
	// or "feed".
	Deliver string
	// ExecCmd is the command exec/resume spawns per wake; the envelope
	// goes to its stdin.
	ExecCmd []string
	// SocketURL is the POST endpoint for the socket backend.
	SocketURL string
	// Peers distinguishes bots from humans (the firebreak).
	Peers wake.Peers
	// Self is the bot's own identity.
	Self wake.Self
	// Ambient configures the ambient gate. An empty Endpoint disables
	// the gate: no ambient evaluation happens. The Slack transport
	// forces the gate off regardless: Slack has no quiet/mute mapping,
	// so ambient wakes there could not be silenced.
	Ambient AmbientConfig
}

// AmbientConfig configures the ambient wake gate. The classifier request
// is POST JSON {"from": <user_id>, "text": <text>}; the response is
// {"directed": bool, "p": <0..1>} — p wins when present.
type AmbientConfig struct {
	// Endpoint is the classifier URL; "" disables ambient wakes.
	Endpoint string
	// Threshold is the directed-at-me probability cutoff when the
	// classifier returns p.
	Threshold float64
	// MinSpacing is the minimum spacing between ambient wakes; 0 (or a
	// negative) defaults to DefaultMinSpacing (30m) when the gate is
	// enabled.
	MinSpacing time.Duration
	// MaxPerHour caps ambient wakes in any trailing hour.
	MaxPerHour int
}

// Defaults for unset configuration.
const (
	DefaultAgent        = "resident"
	DefaultMode         = ModeResident
	DefaultDeliver      = DeliverExec
	DefaultThreshold    = 0.5
	DefaultMinSpacing   = 30 * time.Minute
	DefaultMaxPerHour   = 4
	DefaultSubscribeTTL = 86400 // seconds; one day
	ExpirySweepInterval = time.Minute
	ExecWaitTimeout     = 30 * time.Second
	SocketTimeout       = 10 * time.Second
	ClassifierTimeout   = 10 * time.Second
	HistoryLookupLimit  = 50
	MaxImageBytes       = 20 << 20 // 20 MiB, the server upload cap
)

// WithDefaults fills unset fields: Agent "resident", Mode "resident",
// Deliver "exec", and the ambient valve defaults (threshold, min
// spacing, per-hour cap) when the gate is enabled.
func (c Config) WithDefaults() Config {
	if c.Agent == "" {
		c.Agent = DefaultAgent
	}
	if c.Transport == "" {
		c.Transport = TransportApron
	}
	if c.Mode == "" {
		c.Mode = DefaultMode
	}
	if c.Deliver == "" {
		c.Deliver = DefaultDeliver
	}
	if c.Ambient.Endpoint != "" {
		if c.Ambient.Threshold == 0 {
			c.Ambient.Threshold = DefaultThreshold
		}
		if c.Ambient.MaxPerHour == 0 {
			c.Ambient.MaxPerHour = DefaultMaxPerHour
		}
		if c.Ambient.MinSpacing <= 0 {
			c.Ambient.MinSpacing = DefaultMinSpacing
		}
	}
	return c
}

// Validate rejects an unknown mode, delivery backend, or transport.
// An empty self identity is rejected for apron: with an empty user id
// the self-post firebreak (m.From.UserID == Self.UserID) matches every
// post, and the daemon would evaluate its own posts and self-wake. The
// slack transport may leave it empty: Run resolves the bot's user id
// from auth.test at startup.
func (c Config) Validate() error {
	switch c.Transport {
	case "", TransportApron, TransportSlack:
	default:
		return fmt.Errorf("unknown transport %q (want %q or %q)", c.Transport, TransportApron, TransportSlack)
	}
	if c.Self.UserID == "" && c.Transport != TransportSlack {
		return fmt.Errorf("self.user_id is empty: set it to the bot's apron user id (the self-post firebreak compares against it)")
	}
	switch c.Mode {
	case ModeResident, ModeShared:
	default:
		return fmt.Errorf("unknown mode %q (want %q or %q)", c.Mode, ModeResident, ModeShared)
	}
	switch c.Deliver {
	case DeliverExec, DeliverResume, DeliverSocket, DeliverFeed:
	default:
		return fmt.Errorf("unknown deliver backend %q (want exec, resume, socket, or feed)", c.Deliver)
	}
	return nil
}

// ValidateBackend rejects the backend/endpoint combinations no backend
// can serve, and a slack transport missing either token. Run calls it
// before it connects, so a misconfigured backend is rejected before the
// first wake can panic the deliverer.
func (c Config) ValidateBackend() error {
	if c.Transport == TransportSlack {
		if c.BotToken == "" || c.AppToken == "" {
			return fmt.Errorf("transport %q needs both SLACK_BOT_TOKEN and SLACK_APP_TOKEN", c.Transport)
		}
	}
	switch c.Deliver {
	case DeliverExec, DeliverResume:
		if len(c.ExecCmd) == 0 {
			return fmt.Errorf("deliver %q needs an exec command", c.Deliver)
		}
	case DeliverSocket:
		if c.SocketURL == "" {
			return fmt.Errorf("deliver %q needs a socket URL", c.Deliver)
		}
	}
	return nil
}
