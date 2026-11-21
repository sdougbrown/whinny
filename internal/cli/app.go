// Package cli builds the whinny command surface: the clicontract app,
// the command handlers, the room-resolution ladder, and config
// resolution. Handlers take a narrow RoomClient and store dependencies,
// so tests fake the apron connection without a network.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/cli-contract/registry"
	"github.com/sdougbrown/whinny/internal/daemon"
	"github.com/sdougbrown/whinny/internal/store"
)

// Name is the CLI name.
const Name = "whinny"

// Version is the CLI version; release builds set it via -ldflags.
var Version = "0.1.0"

// Deps carries the runtime dependencies the handlers use.
type Deps struct {
	// Client is a pre-connected room client (tests); when nil, the
	// handler connects through Connect.
	Client RoomClient
	// Connect dials apron; production only.
	Connect func(ctx context.Context, cfg Config) (RoomClient, error)
	// Store is a pre-opened state store (tests); when nil, the handler
	// opens one through OpenStore.
	Store     *store.Store
	OpenStore func() (*store.Store, error)
	// Env looks up environment variables; nil uses os.LookupEnv.
	Env func(string) string
	// Now is the wall clock for guard evaluation; nil uses time.Now.
	Now func() time.Time
	// Stdin is read for `post -`; nil reads os.Stdin.
	Stdin io.Reader
	// RunDaemon is the daemon entry point the daemon command calls;
	// nil uses daemon.Run (tests stub it).
	RunDaemon func(ctx context.Context, cfg daemon.Config, deps daemon.Deps) error
}

func (d Deps) env() func(string) string {
	if d.Env != nil {
		return d.Env
	}
	return func(k string) string { v, _ := os.LookupEnv(k); return v }
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// connect returns the room client the handler uses and whether the
// handler owns it (and must Close it).
func (d Deps) connect(ctx context.Context, inv clicontract.Invocation) (RoomClient, bool, error) {
	if d.Client != nil {
		return d.Client, false, nil
	}
	cfg, err := ResolveConfig(inv.Flags["url"], inv.Flags["token"], d.env())
	if err != nil {
		return nil, false, err
	}
	if d.Connect == nil {
		return nil, false, OperationalErrorf("no apron connection available")
	}
	c, err := d.Connect(ctx, cfg)
	if err != nil {
		return nil, false, OperationalErrorf("connect to %s: %v", cfg.URL, err)
	}
	return c, true, nil
}

// store returns the state store the handler uses.
func (d Deps) store() (*store.Store, error) {
	if d.Store != nil {
		return d.Store, nil
	}
	if d.OpenStore == nil {
		return nil, OperationalErrorf("no state store configured")
	}
	return d.OpenStore()
}

// App builds the declared whinny CLI.
func App(d Deps) *clicontract.App {
	var self *clicontract.App
	self = &clicontract.App{
		Name:        Name,
		Version:     Version,
		RootShort:   "talk to apron rooms from the command line",
		ErrorPrefix: Name,
		// The daemon command installs its own SIGINT/SIGTERM wiring so it
		// can exit 0 on shutdown; the library stays out of the way for
		// every command.
		HandlerOwnedSignals: true,
		CommonFlags: []clicontract.Flag{
			{Name: "url", Kind: clicontract.String, Usage: "apron endpoint (default $WHINNY_URL, $APRON_URL, " + DefaultURL + ")"},
			{Name: "token", Kind: clicontract.String, Usage: "apron auth token (default $WHINNY_TOKEN, $APRON_TOKEN)"},
		},
		Commands: []*clicontract.Command{
			{
				Name:        "post",
				Short:       "post a message to a room",
				Positionals: []clicontract.Positional{{Name: "text", Required: true}},
				Flags: []clicontract.Flag{
					{Name: "room", Kind: clicontract.String, Required: true, Usage: "target room id or title"},
					{Name: "reply-to", Kind: clicontract.String, Usage: "message_id this post replies to"},
					{Name: "thread-of", Kind: clicontract.String, Usage: "message_id to thread: create a thread and post the first reply anchored to it"},
					{Name: "json", Kind: clicontract.Bool, Usage: "print the result as JSON"},
				},
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
				Handle: PostHandler(d),
			},
			{
				Name:  "history",
				Short: "print a room's recent messages",
				Flags: []clicontract.Flag{
					{Name: "room", Kind: clicontract.String, Required: true, Usage: "room id or title"},
					{Name: "limit", Kind: clicontract.Int, Default: DefaultHistoryLimit, Usage: fmt.Sprintf("maximum messages to fetch (default %d)", DefaultHistoryLimit)},
					{Name: "before", Kind: clicontract.String, Usage: "message_id cursor: only messages strictly older than it"},
					{Name: "since", Kind: clicontract.String, Usage: "message_id cursor: only messages strictly newer than it"},
					{Name: "json", Kind: clicontract.Bool, Usage: "print the messages as a JSON array"},
				},
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
				Handle: HistoryHandler(d),
			},
			{
				Name:  "rooms",
				Short: "list joined and not-joined rooms",
				Flags: []clicontract.Flag{
					{Name: "json", Kind: clicontract.Bool, Usage: "print the rooms as JSON"},
				},
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
				Handle: RoomsHandler(d),
			},
			{
				Name:  "watch",
				Short: "manage wake watches",
				Subcommands: []*clicontract.Command{
					{
						Name:  "add",
						Short: "add a wake watch",
						Flags: []clicontract.Flag{
							{Name: "room", Kind: clicontract.String, Required: true, Usage: "room id or title"},
							{Name: "from", Kind: clicontract.String, Usage: "only messages from this user_id"},
							{Name: "pattern", Kind: clicontract.String, Usage: "regex to match in message text or embeds"},
							{Name: "thread", Kind: clicontract.String, Usage: "message_id of the thread root to watch"},
							{Name: "reply-to", Kind: clicontract.String, Usage: "message_id to watch for a reply to"},
							{Name: "note", Kind: clicontract.String, Usage: "note carried by the wake decision"},
							{Name: "once", Kind: clicontract.Bool, Usage: "watch fires once (default)"},
							{Name: "persistent", Kind: clicontract.Bool, Usage: "watch stays until it expires or is cancelled"},
							{Name: "ttl", Kind: clicontract.Duration, Default: 24 * time.Hour, Usage: "watch time to live (default 24h, cap 168h)"},
							{Name: "owner", Kind: clicontract.String, Usage: "transient identity the watch belongs to (shared mode); requires --on-fire"},
							{Name: "on-fire", Kind: clicontract.String, Usage: "delivery command the watch's hits fire (shared mode); requires --owner"},
						},
						Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
						Handle: AddWatchHandler(d),
					},
					{
						Name:        "cancel",
						Short:       "cancel a wake watch by id",
						Positionals: []clicontract.Positional{{Name: "id", Required: true}},
						Output:      clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
						Handle:      CancelWatchHandler(d),
					},
					{
						Name:  "list",
						Short: "list wake watches",
						Flags: []clicontract.Flag{
							{Name: "json", Kind: clicontract.Bool, Usage: "print the watches as JSON"},
						},
						Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
						Handle: ListWatchesHandler(d),
					},
				},
			},
			{
				Name:  "quiet",
				Short: "gate the bot's sends for a duration",
				Long: "Set the send-path quiet gate: posts are refused until the quiet period ends. " +
					"--for sets the duration (default 8h); --for 0 is indefinite (a large sentinel). " +
					"Records keep flowing; quiet only gates sends. Use `whinny resume` to clear.",
				Flags: []clicontract.Flag{
					{Name: "for", Kind: clicontract.Duration, Default: DefaultQuietDuration, Usage: fmt.Sprintf("quiet duration (default %dh; 0 = indefinite)", int(DefaultQuietDuration.Hours()))},
				},
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
				Handle: QuietHandler(d),
			},
			{
				Name:   "resume",
				Short:  "clear the send-path quiet gate",
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
				Handle: ResumeHandler(d),
			},
			{
				Name:  "daemon",
				Short: "run the wake daemon (long-running)",
				Long: "Hold the apron connection, evaluate the wake policy over " +
					"inbound messages, and deliver wake envelopes through the " +
					"configured backend. Modes: resident (full policy, durable " +
					"wake queue) or shared (watch hits only, fire-and-forget). " +
					"Backends: exec and resume spawn --exec-cmd with the envelope " +
					"on stdin; socket POSTs JSON to --socket-url; feed writes " +
					"JSON lines to stdout (the poo feed contract). " +
					"SIGINT/SIGTERM shut the daemon down cleanly; the process " +
					"exits 0.",
				Flags: []clicontract.Flag{
					{Name: "mode", Kind: clicontract.String, Usage: "resident or shared (default resident; $WHINNY_DAEMON_MODE)"},
					{Name: "deliver", Kind: clicontract.String, Usage: "exec, resume, socket, or feed (default exec; $WHINNY_DAEMON_DELIVER)"},
					{Name: "exec-cmd", Kind: clicontract.String, Usage: "command exec/resume spawns per wake; envelope on stdin (shell-split)"},
					{Name: "socket-url", Kind: clicontract.String, Usage: "POST endpoint for the socket backend"},
					{Name: "config", Kind: clicontract.String, Usage: "daemon config JSON (default $WHINNY_CONFIG, ~/.config/whinny/daemon.json; non-secret parts)"},
				},
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
				Raw:    DaemonHandler(d),
			},
			{
				Name:  "schema",
				Short: "print this tool's schema document",
				Raw: func(ctx context.Context, inv clicontract.Invocation, stdout io.Writer) error {
					doc, err := self.SchemaDocument()
					if err != nil {
						return err
					}
					enc := json.NewEncoder(stdout)
					enc.SetIndent("", "  ")
					return enc.Encode(doc)
				},
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
			},
			{
				Name:  "manifest",
				Short: "print this tool's cli-registry manifest",
				Raw: func(ctx context.Context, inv clicontract.Invocation, stdout io.Writer) error {
					b, err := registry.Manifest(self, registry.Options{Binary: Name, CommonFlags: []string{"url", "token"}})
					if err != nil {
						return err
					}
					_, err = stdout.Write(b)
					return err
				},
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
			},
			{
				Name:  "version",
				Short: "print the whinny name and version",
				Raw: func(ctx context.Context, inv clicontract.Invocation, stdout io.Writer) error {
					fmt.Fprintf(stdout, "%s %s\n", self.Name, self.Version)
					return nil
				},
				Output: clicontract.Output{Mode: clicontract.OutputRaw, SuccessExitCodes: []int{0}},
			},
		},
	}
	return self
}
